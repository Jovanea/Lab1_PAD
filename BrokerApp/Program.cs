using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;

namespace BrokerApp;

public class Message
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string Topic { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}

public class Packet
{
    public string Action { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public Message? MessageData { get; set; }
}

public class BrokerResponse
{
    public bool Success { get; set; }
    public string Code { get; set; } = string.Empty;
    public string Detail { get; set; } = string.Empty;
}

public class BrokerTopicsResponse : BrokerResponse
{
    public List<string> Topics { get; set; } = new();
}

public sealed class DeadLetterRecord
{
    public string Id { get; init; } = Guid.NewGuid().ToString();
    public string ClientId { get; init; } = string.Empty;
    public Message Message { get; init; } = new();
    public string Reason { get; init; } = string.Empty;
    public DateTime FailedAt { get; init; } = DateTime.Now;
    public int Attempts { get; init; }
}

public record BrokerActivity(string Category, string Detail, DateTime Time);
public record BrokerSubscriber(string ClientId, string Topic, bool IsConnected);
public record BrokerDeadLetter(string Id, string ClientId, string Topic, string Payload, string Reason, int Attempts, DateTime FailedAt);
public record BrokerSnapshot(int SubscriberCount, int ConnectedCount, int PendingCount, IReadOnlyList<BrokerSubscriber> Subscribers, int DeadLetterCount, IReadOnlyList<BrokerDeadLetter> DeadLetters, int OrphanCount);

public sealed class BrokerServer : IDisposable
{
    private static readonly Encoding Utf8 = new UTF8Encoding(false);
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private const int MaxDeliveryAttempts = 5;
    private readonly ConcurrentDictionary<string, HashSet<string>> _subscriptions = new();
    private readonly ConcurrentDictionary<string, ConcurrentQueue<Message>> _pendingMessages = new();
    private readonly ConcurrentDictionary<string, TcpClient> _activeSockets = new();
    private readonly ConcurrentDictionary<string, StreamReader> _activeReaders = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _deliveryLocks = new();
    private readonly ConcurrentDictionary<string, int> _deliveryAttempts = new();
    private readonly ConcurrentDictionary<string, DeadLetterRecord> _deadLetterMessages = new();

    // mesaje publicate fara niciun abonat, pana cineva se aboneaza la topicul lor
    private readonly ConcurrentDictionary<string, ConcurrentQueue<Message>> _orphanMessages = new(StringComparer.OrdinalIgnoreCase);

    // istoricul complet al mesajelor publicate per topic, pentru a le livra noilor abonati
    private readonly ConcurrentDictionary<string, List<Message>> _topicHistory = new(StringComparer.OrdinalIgnoreCase);

    // inregistrarea mesajelor livrate/asignate per client, pentru a preveni duplicatele
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _deliveredMessages = new(StringComparer.OrdinalIgnoreCase);
    private TcpListener? _listener;
    private CancellationTokenSource? _cancellation;

    public bool IsRunning { get; private set; }
    public event EventHandler<BrokerActivity>? Activity;

    public Task StartAsync(int port)
    {
        if (IsRunning)
        {
            return Task.CompletedTask;
        }

        _cancellation = new CancellationTokenSource();
        _listener = new TcpListener(IPAddress.Any, port);
        _listener.Start();
        IsRunning = true;
        WriteActivity("SYSTEM", $"Broker pornit pe portul TCP {port}.");
        _ = Task.Run(() => AcceptLoopAsync(_cancellation.Token));
        _ = Task.Run(() => DeliveryWorkerAsync(_cancellation.Token));
        return Task.CompletedTask;
    }

    public Task StopAsync()
    {
        if (!IsRunning)
        {
            return Task.CompletedTask;
        }

        IsRunning = false;
        _cancellation?.Cancel();
        _listener?.Stop();
        foreach (TcpClient client in _activeSockets.Values)
        {
            client.Close();
        }
        _activeSockets.Clear();
        _activeReaders.Clear();

        _subscriptions.Clear();
        _pendingMessages.Clear();
        _deliveryAttempts.Clear();
        _deadLetterMessages.Clear();
        _orphanMessages.Clear();
        _topicHistory.Clear();
        _deliveredMessages.Clear();

        WriteActivity("SYSTEM", "Broker oprit; toata starea a fost stearsa.");
        return Task.CompletedTask;
    }

    public BrokerSnapshot GetSnapshot()
    {
        List<BrokerSubscriber> subscribers = _subscriptions
            .OrderBy(item => item.Key)
            .Select(item => new BrokerSubscriber(
                item.Key,
                string.Join(", ", item.Value.OrderBy(topic => topic, StringComparer.OrdinalIgnoreCase)),
                _activeSockets.ContainsKey(item.Key)))
            .ToList();

        List<BrokerDeadLetter> deadLetters = _deadLetterMessages.Values
            .OrderByDescending(record => record.FailedAt)
            .Select(record => new BrokerDeadLetter(record.Id, record.ClientId, record.Message.Topic, record.Message.Payload, record.Reason, record.Attempts, record.FailedAt))
            .ToList();

        return new BrokerSnapshot(
            subscribers.Count,
            _activeSockets.Count,
            _pendingMessages.Sum(item => item.Value.Count),
            subscribers,
            deadLetters.Count,
            deadLetters,
            _orphanMessages.Sum(item => item.Value.Count));
    }

    public bool RequeueDeadLetter(string id)
    {
        if (!_deadLetterMessages.TryRemove(id, out DeadLetterRecord? record))
        {
            return false;
        }

        _pendingMessages.GetOrAdd(record.ClientId, _ => new ConcurrentQueue<Message>()).Enqueue(record.Message);
        WriteActivity("REQUEUE", $"Mesaj readaugat in coada pentru {record.ClientId}.");
        _ = Task.Run(() => DeliverPendingMessagesForClient(record.ClientId));
        return true;
    }

    public bool DiscardDeadLetter(string id)
    {
        if (!_deadLetterMessages.TryRemove(id, out _))
        {
            return false;
        }

        WriteActivity("DEAD_LETTER", "Mesaj sters definitiv din dead-letter.");
        return true;
    }

    public void ClearDeadLetters()
    {
        if (_deadLetterMessages.IsEmpty)
        {
            return;
        }

        _deadLetterMessages.Clear();
        WriteActivity("DEAD_LETTER", "Coada dead-letter a fost golita.");
    }

    public void ClearAllSubscribers()
    {
        if (_subscriptions.IsEmpty && _activeSockets.IsEmpty && _pendingMessages.IsEmpty)
        {
            return;
        }

        foreach (TcpClient client in _activeSockets.Values)
        {
            try
            {
                client.Close();
            }
            catch (Exception)
            {
            }
        }

        _activeSockets.Clear();
        _activeReaders.Clear();
        _subscriptions.Clear();
        _pendingMessages.Clear();
        _deliveryAttempts.Clear();
        _topicHistory.Clear();
        _deliveredMessages.Clear();
        WriteActivity("SYSTEM", "Toti abonatii au fost eliminati; brokerul a fost resetat la 0 receivere.");
    }

    public bool SimulateDeliveryFailure(string clientId)
    {
        if (!_pendingMessages.TryGetValue(clientId, out ConcurrentQueue<Message>? queue) || !queue.TryPeek(out Message? message))
        {
            WriteActivity("SYSTEM", $"Nu exista niciun mesaj pending pentru {clientId} de simulat ca esuat.");
            return false;
        }

        string attemptKey = $"{clientId}:{message.Id}";
        HandleDeliveryFailure(clientId, queue, message, attemptKey, "Esec simulat manual (test).");
        return true;
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && _listener is not null)
        {
            try
            {
                TcpClient client = await _listener.AcceptTcpClientAsync(cancellationToken);
                _ = Task.Run(() => HandleClient(client, cancellationToken), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (SocketException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                WriteActivity("ERROR", $"Eroare la acceptarea conexiunii: {ex.Message}");
            }
        }
    }

    private void HandleClient(TcpClient client, CancellationToken cancellationToken)
    {
        using (client)
        using (NetworkStream stream = client.GetStream())
        using (StreamReader reader = new(stream, Utf8, leaveOpen: true))
        {
            try
            {
                string? rawData = reader.ReadLine();
                if (string.IsNullOrWhiteSpace(rawData))
                {
                    SendResponse(stream, false, "EMPTY_PACKET", "Pachetul nu poate fi gol.");
                    return;
                }

                Packet? packet;
                try
                {
                    packet = JsonSerializer.Deserialize<Packet>(rawData, JsonOptions);
                }
                catch (JsonException)
                {
                    SendResponse(stream, false, "INVALID_JSON", "Mesajul trebuie sa fie JSON valid.");
                    WriteActivity("ERROR", "A fost respins un pachet JSON invalid.");
                    return;
                }

                if (packet is null || string.IsNullOrWhiteSpace(packet.Action))
                {
                    SendResponse(stream, false, "INVALID_PACKET", "Action este obligatoriu.");
                    return;
                }

                switch (packet.Action.ToUpperInvariant())
                {
                    case "LIST_TOPICS":
                        HandleListTopics(stream);
                        return;
                    case "PUBLISH":
                        if (string.IsNullOrWhiteSpace(packet.Topic))
                        {
                            SendResponse(stream, false, "INVALID_PACKET", "Topic este obligatoriu pentru PUBLISH.");
                            return;
                        }
                        HandlePublish(packet, stream);
                        return;
                    case "SUBSCRIBE":
                        if (string.IsNullOrWhiteSpace(packet.Topic))
                        {
                            SendResponse(stream, false, "INVALID_PACKET", "Topic este obligatoriu pentru SUBSCRIBE.");
                            return;
                        }
                        HandleSubscribe(packet, client, stream, reader, cancellationToken);
                        return;
                    case "ADD_TOPIC":
                        if (string.IsNullOrWhiteSpace(packet.Topic))
                        {
                            SendResponse(stream, false, "INVALID_PACKET", "Topic este obligatoriu pentru ADD_TOPIC.");
                            return;
                        }
                        HandleAddTopic(packet, stream);
                        return;
                    default:
                        SendResponse(stream, false, "UNKNOWN_ACTION", "Action acceptat: PUBLISH, SUBSCRIBE, ADD_TOPIC sau LIST_TOPICS.");
                        return;
                }
            }
            catch (IOException)
            {
            }
            catch (SocketException)
            {
            }
        }
    }

    private void HandlePublish(Packet packet, NetworkStream stream)
    {
        Message? message = packet.MessageData;
        if (message is null || string.IsNullOrWhiteSpace(message.Id) || string.IsNullOrWhiteSpace(message.Payload) ||
            !string.Equals(packet.Topic, message.Topic, StringComparison.OrdinalIgnoreCase))
        {
            SendResponse(stream, false, "INVALID_MESSAGE", "Mesajul trebuie sa aiba Id, Payload si acelasi Topic ca pachetul.");
            return;
        }

        // pastreaza mesajul in istoricul topicului pentru viitorii abonati
        _topicHistory.AddOrUpdate(
            packet.Topic,
            _ => new List<Message> { message },
            (_, list) => { lock (list) { list.Add(message); } return list; });

        string[] recipients = _subscriptions
            .Where(item => item.Value.Contains(packet.Topic))
            .Select(item => item.Key)
            .ToArray();

        foreach (string clientId in recipients)
        {
            _deliveredMessages.GetOrAdd(clientId, _ => new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase)).TryAdd(message.Id, 0);
            _pendingMessages.GetOrAdd(clientId, _ => new ConcurrentQueue<Message>()).Enqueue(message);
            _ = Task.Run(() => DeliverPendingMessagesForClient(clientId));
        }

        if (recipients.Length == 0)
        {
            _orphanMessages.GetOrAdd(packet.Topic, _ => new ConcurrentQueue<Message>()).Enqueue(message);
            SendResponse(stream, true, "PUBLISH_ACCEPTED", "Mesaj acceptat, dar momentan nu exista niciun abonat pe acest topic; va fi livrat cand cineva se aboneaza.");
            WriteActivity("PUBLISH", $"Topic „{packet.Topic}” publicat, dar fara niciun abonat momentan (mesaj pastrat pentru viitor).");
            return;
        }

        SendResponse(stream, true, "PUBLISH_ACCEPTED", "Mesaj validat si stocat pentru livrare.");
        WriteActivity("PUBLISH", $"Topic „{packet.Topic}” trimis catre {recipients.Length} abonat(i).");
    }

    private void HandleListTopics(NetworkStream stream)
    {
        List<string> topics = GetActiveTopics();
        byte[] response = Utf8.GetBytes(JsonSerializer.Serialize(new BrokerTopicsResponse
        {
            Success = true,
            Code = "TOPICS",
            Detail = topics.Count == 0 ? "Nu exista topicuri active." : "Topicuri active returnate.",
            Topics = topics
        }) + "\n");
        stream.Write(response, 0, response.Length);
        stream.Flush();
    }

    private void HandleSubscribe(Packet packet, TcpClient client, NetworkStream stream, StreamReader reader, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(packet.ClientId))
        {
            SendResponse(stream, false, "INVALID_SUBSCRIPTION", "ClientId este obligatoriu.");
            return;
        }

        HashSet<string> topics = ParseTopics(packet.Topic);
        if (topics.Count == 0)
        {
            SendResponse(stream, false, "INVALID_PACKET", "Cel putin un topic valid este obligatoriu pentru SUBSCRIBE.");
            return;
        }

        if (_activeSockets.ContainsKey(packet.ClientId))
        {
            SendResponse(stream, false, "CLIENT_ID_TAKEN",
                $"Numele de client '{packet.ClientId}' este deja folosit de un receiver conectat activ. Alege alt Client ID.");
            return;
        }

        _subscriptions[packet.ClientId] = topics;
        _activeSockets[packet.ClientId] = client;
        _activeReaders[packet.ClientId] = reader;
        foreach (string topic in topics)
        {
            ClaimOrphanMessagesForTopic(packet.ClientId, topic);
        }
        // livreaza istoricul mesajelor anterioare pentru topicele abonate
        ReplayHistoryForClient(packet.ClientId, topics);
        string joinedTopics = string.Join(", ", topics.OrderBy(topic => topic, StringComparer.OrdinalIgnoreCase));
        SendResponse(stream, true, "SUBSCRIBED", $"Abonat la topicurile: {joinedTopics}.");
        WriteActivity("SUBSCRIBE", $"{packet.ClientId} este abonat la „{joinedTopics}”.");
        DeliverPendingMessagesForClient(packet.ClientId);

        while (!cancellationToken.IsCancellationRequested && IsSocketConnected(client))
        {
            Thread.Sleep(500);
        }

        RemoveActiveConnection(packet.ClientId, client);
    }

    private void HandleAddTopic(Packet packet, NetworkStream stream)
    {
        if (string.IsNullOrWhiteSpace(packet.ClientId))
        {
            SendResponse(stream, false, "INVALID_SUBSCRIPTION", "ClientId este obligatoriu.");
            return;
        }

        if (!_subscriptions.TryGetValue(packet.ClientId, out HashSet<string>? existingTopics))
        {
            SendResponse(stream, false, "UNKNOWN_CLIENT", "Clientul trebuie sa fie deja abonat (SUBSCRIBE) inainte de a adauga topicuri noi.");
            return;
        }

        HashSet<string> requestedTopics = ParseTopics(packet.Topic);
        if (requestedTopics.Count == 0)
        {
            SendResponse(stream, false, "INVALID_PACKET", "Cel putin un topic valid este obligatoriu.");
            return;
        }

        string[] newTopics = requestedTopics.Where(topic => !existingTopics.Contains(topic)).ToArray();
        if (newTopics.Length == 0)
        {
            SendResponse(stream, false, "ALREADY_SUBSCRIBED",
                $"Esti deja abonat la: {string.Join(", ", requestedTopics.OrderBy(t => t, StringComparer.OrdinalIgnoreCase))}. Nu s-a adaugat nimic nou.");
            return;
        }

        HashSet<string> topics = _subscriptions.AddOrUpdate(
            packet.ClientId,
            _ => new HashSet<string>(newTopics, StringComparer.OrdinalIgnoreCase),
            (_, existing) =>
            {
                var merged = new HashSet<string>(existing, StringComparer.OrdinalIgnoreCase);
                merged.UnionWith(newTopics);
                return merged;
            });

        foreach (string topic in newTopics)
        {
            ClaimOrphanMessagesForTopic(packet.ClientId, topic);
        }
        // livreaza istoricul mesajelor anterioare pentru topicele nou adaugate
        ReplayHistoryForClient(packet.ClientId, new HashSet<string>(newTopics, StringComparer.OrdinalIgnoreCase));

        string joinedTopics = string.Join(", ", topics.OrderBy(topic => topic, StringComparer.OrdinalIgnoreCase));
        string joinedNewTopics = string.Join(", ", newTopics.OrderBy(t => t, StringComparer.OrdinalIgnoreCase));
        SendResponse(stream, true, "TOPIC_ADDED", $"Adaugat: {joinedNewTopics}. Topicuri curente: {joinedTopics}.");
        WriteActivity("SUBSCRIBE", $"{packet.ClientId} a adaugat topicul(urile) „{joinedNewTopics}”. Total abonamente: {joinedTopics}.");
        DeliverPendingMessagesForClient(packet.ClientId);
    }

    private void DeliverPendingMessagesForClient(string clientId)
    {
        SemaphoreSlim deliveryLock = _deliveryLocks.GetOrAdd(clientId, _ => new SemaphoreSlim(1, 1));
        deliveryLock.Wait();
        try
        {
            if (!_activeSockets.TryGetValue(clientId, out TcpClient? client) || !IsSocketConnected(client) ||
                !_activeReaders.TryGetValue(clientId, out StreamReader? ackReader) ||
                !_pendingMessages.TryGetValue(clientId, out ConcurrentQueue<Message>? queue))
            {
                return;
            }

            while (queue.TryPeek(out Message? message))
            {
                string attemptKey = $"{clientId}:{message.Id}";
                try
                {
                    NetworkStream stream = client.GetStream();
                    byte[] data = Utf8.GetBytes(JsonSerializer.Serialize(message) + "\n");
                    stream.Write(data, 0, data.Length);
                    stream.Flush();
                    stream.ReadTimeout = 5000;

                    if (ackReader.ReadLine() != $"ACK:{message.Id}")
                    {
                        RemoveActiveConnection(clientId, client);
                        HandleDeliveryFailure(clientId, queue, message, attemptKey, "Nu s-a primit confirmarea ACK.");
                        break;
                    }

                    queue.TryDequeue(out _);
                    _deliveryAttempts.TryRemove(attemptKey, out _);
                    WriteActivity("DELIVERED", $"Mesaj livrat si confirmat de {clientId}.");
                }
                catch (IOException)
                {
                    RemoveActiveConnection(clientId, client);
                    HandleDeliveryFailure(clientId, queue, message, attemptKey, "Eroare IO la trimiterea mesajului.");
                    break;
                }
                catch (SocketException)
                {
                    RemoveActiveConnection(clientId, client);
                    HandleDeliveryFailure(clientId, queue, message, attemptKey, "Eroare de socket la trimiterea mesajului.");
                    break;
                }
            }
        }
        finally
        {
            deliveryLock.Release();
        }
    }

    private void HandleDeliveryFailure(string clientId, ConcurrentQueue<Message> queue, Message message, string attemptKey, string reason)
    {
        int attempts = _deliveryAttempts.AddOrUpdate(attemptKey, 1, (_, count) => count + 1);
        if (attempts < MaxDeliveryAttempts)
        {
            WriteActivity("WAITING", $"{clientId} nu raspunde (incercarea {attempts}/{MaxDeliveryAttempts}); mesajul ramane pending.");
            return;
        }

        _deliveryAttempts.TryRemove(attemptKey, out _);
        if (queue.TryDequeue(out _))
        {
            var record = new DeadLetterRecord
            {
                ClientId = clientId,
                Message = message,
                Reason = reason,
                FailedAt = DateTime.Now,
                Attempts = attempts
            };
            _deadLetterMessages[record.Id] = record;
            WriteActivity("DEAD_LETTER", $"Mesaj catre {clientId} mutat in dead-letter dupa {attempts} incercari esuate ({reason})");
        }
    }

    private async Task DeliveryWorkerAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (string clientId in _activeSockets.Keys)
            {
                _ = Task.Run(() => DeliverPendingMessagesForClient(clientId), cancellationToken);
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private static void SendResponse(NetworkStream stream, bool success, string code, string detail)
    {
        byte[] response = Utf8.GetBytes(JsonSerializer.Serialize(new BrokerResponse
        {
            Success = success,
            Code = code,
            Detail = detail
        }) + "\n");
        stream.Write(response, 0, response.Length);
        stream.Flush();
    }

    private List<string> GetActiveTopics()
    {
        CleanupInactiveConnections();

        return _activeSockets.Keys
            .Where(clientId => _subscriptions.ContainsKey(clientId))
            .SelectMany(clientId => _subscriptions[clientId])
            .Where(topic => !string.IsNullOrWhiteSpace(topic))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(topic => topic, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static HashSet<string> ParseTopics(string rawTopic)
    {
        var topics = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (string candidate in (rawTopic ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            topics.Add(candidate);
        }

        return topics;
    }

    private void ReplayHistoryForClient(string clientId, HashSet<string> topics)
    {
        int replayed = 0;
        ConcurrentQueue<Message> queue = _pendingMessages.GetOrAdd(clientId, _ => new ConcurrentQueue<Message>());
        ConcurrentDictionary<string, byte> clientDelivered = _deliveredMessages.GetOrAdd(clientId, _ => new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase));

        foreach (string topic in topics)
        {
            if (_topicHistory.TryGetValue(topic, out List<Message>? history))
            {
                lock (history)
                {
                    foreach (Message message in history)
                    {
                        if (clientDelivered.TryAdd(message.Id, 0))
                        {
                            queue.Enqueue(message);
                            replayed++;
                        }
                    }
                }
            }
        }

        if (replayed > 0)
        {
            WriteActivity("REPLAY", $"{clientId} a primit {replayed} mesaj(e) din istoricul topicelor.");
        }
    }

    private void ClaimOrphanMessagesForTopic(string clientId, string topic)
    {
        if (!_orphanMessages.TryRemove(topic, out _))
        {
            return;
        }

        WriteActivity("PUBLISH", $"Topic „{topic}” are acum cel putin un abonat ({clientId}); starea fara abonati a fost resetata.");
    }

    private void CleanupInactiveConnections()
    {
        foreach ((string clientId, TcpClient client) in _activeSockets)
        {
            if (!IsSocketConnected(client))
            {
                RemoveActiveConnection(clientId, client);
            }
        }
    }

    private static bool IsSocketConnected(TcpClient client)
    {
        try
        {
            Socket socket = client.Client;
            return client.Connected && !(socket.Poll(0, SelectMode.SelectRead) && socket.Available == 0);
        }
        catch (SocketException)
        {
            return false;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }

    private void RemoveActiveConnection(string clientId, TcpClient expectedClient)
    {
        if (_activeSockets.TryGetValue(clientId, out TcpClient? activeClient) && ReferenceEquals(activeClient, expectedClient))
        {
            _activeSockets.TryRemove(clientId, out _);
            _activeReaders.TryRemove(clientId, out _);
            WriteActivity("OFFLINE", $"{clientId} este deconectat.");
        }
    }

    private void WriteActivity(string category, string detail) => Activity?.Invoke(this, new BrokerActivity(category, detail, DateTime.Now));

    public void Dispose()
    {
        _ = StopAsync();
        _cancellation?.Dispose();
        foreach (SemaphoreSlim deliveryLock in _deliveryLocks.Values)
        {
            deliveryLock.Dispose();
        }
    }
}

internal static class Program
{
    [STAThread]
    public static void Main(string[] args) => BuildAvaloniaApp()
        .StartWithClassicDesktopLifetime(args);

    public static AppBuilder BuildAvaloniaApp()
        => AppBuilder.Configure<App>()
            .UsePlatformDetect()
            .LogToTrace();
}
