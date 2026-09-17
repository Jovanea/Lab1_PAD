using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Xml.Serialization;
using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using MessageHub.Configuration;

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

public class SubscriptionItem
{
    public string ClientId { get; set; } = string.Empty;

    // Camp vechi, pastrat doar pentru compatibilitate cu fisiere broker_storage.xml
    // scrise inainte de suportul pentru abonare la mai multe topicuri. Nu mai este scris la salvare.
    public string? Topic { get; set; }
    public bool ShouldSerializeTopic() => false;

    public List<string> Topics { get; set; } = new();
}

public class PendingMessageItem
{
    public string ClientId { get; set; } = string.Empty;
    public Message Message { get; set; } = new();
}

public class DeadLetterItem
{
    public string Id { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public Message Message { get; set; } = new();
    public string Reason { get; set; } = string.Empty;
    public DateTime FailedAt { get; set; }
    public int Attempts { get; set; }
}

[XmlRoot("BrokerState")]
public class BrokerState
{
    public List<SubscriptionItem> Subscriptions { get; set; } = new();
    public List<PendingMessageItem> PendingMessages { get; set; } = new();
    public List<DeadLetterItem> DeadLetterMessages { get; set; } = new();
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
public record BrokerSnapshot(int SubscriberCount, int ConnectedCount, int PendingCount, IReadOnlyList<BrokerSubscriber> Subscribers, int DeadLetterCount, IReadOnlyList<BrokerDeadLetter> DeadLetters);

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
    private readonly object _fileLock = new();
    private readonly string _xmlPath = Path.Combine(EnvironmentSettings.FindProjectRoot(), "BrokerApp", "broker_storage.xml");
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

        LoadFromXml();
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
        WriteActivity("SYSTEM", "Broker oprit.");
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
            deadLetters);
    }

    /// <summary>Repune un mesaj din dead-letter inapoi in coada de livrare a clientului sau.</summary>
    public bool RequeueDeadLetter(string id)
    {
        if (!_deadLetterMessages.TryRemove(id, out DeadLetterRecord? record))
        {
            return false;
        }

        _pendingMessages.GetOrAdd(record.ClientId, _ => new ConcurrentQueue<Message>()).Enqueue(record.Message);
        SaveToXml();
        WriteActivity("REQUEUE", $"Mesaj readaugat in coada pentru {record.ClientId}.");
        _ = Task.Run(() => DeliverPendingMessagesForClient(record.ClientId));
        return true;
    }

    /// <summary>Sterge definitiv un mesaj din dead-letter, fara a-l mai livra.</summary>
    public bool DiscardDeadLetter(string id)
    {
        if (!_deadLetterMessages.TryRemove(id, out _))
        {
            return false;
        }

        SaveToXml();
        WriteActivity("DEAD_LETTER", "Mesaj sters definitiv din dead-letter.");
        return true;
    }

    /// <summary>Goleste in intregime coada de dead-letter.</summary>
    public void ClearDeadLetters()
    {
        if (_deadLetterMessages.IsEmpty)
        {
            return;
        }

        _deadLetterMessages.Clear();
        SaveToXml();
        WriteActivity("DEAD_LETTER", "Coada dead-letter a fost golita.");
    }

    /// <summary>
    /// Elimina toti abonatii cunoscuti: inchide conexiunile active (receiverele vor vedea
    /// conexiunea inchisa si vor trebui sa se re-aboneze), sterge toate abonamentele si mesajele
    /// pending asociate lor, apoi salveaza starea goala. Dead-letter-ul nu este atins (are propriul
    /// buton de golire), ca sa nu pierzi din greseala mesaje esuate pe care voiai sa le revezi.
    /// </summary>
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
                // Ignoram; oricum eliminam toate referintele mai jos.
            }
        }

        _activeSockets.Clear();
        _activeReaders.Clear();
        _subscriptions.Clear();
        _pendingMessages.Clear();
        _deliveryAttempts.Clear();
        SaveToXml();
        WriteActivity("SYSTEM", "Toti abonatii au fost eliminati; brokerul a fost resetat la 0 receivere.");
    }

    /// <summary>
    /// Declanseaza manual acelasi cod de esec de livrare pe care l-ar produce o conexiune reala cazuta
    /// (<see cref="HandleDeliveryFailure"/>), pentru cel mai vechi mesaj pending al clientului dat.
    /// Util pentru a testa dead-letter queue-ul fara sa trebuiasca sa simulezi o cadere reala de retea
    /// (greu de reprodus fiabil pe localhost). Dupa <see cref="MaxDeliveryAttempts"/> apeluri pe acelasi
    /// mesaj, acesta ajunge in dead-letter exact ca intr-un scenariu real.
    /// </summary>
    public bool SimulateDeliveryFailure(string clientId)
    {
        if (!_pendingMessages.TryGetValue(clientId, out ConcurrentQueue<Message>? queue) || !queue.TryPeek(out Message? message))
        {
            WriteActivity("SYSTEM", $"Nu exista niciun mesaj pending pentru {clientId} de simulat ca esuat.");
            return false;
        }

        string attemptKey = $"{clientId}:{message.Id}";
        HandleDeliveryFailure(clientId, queue, message, attemptKey, "Esec simulat manual (test).");
        SaveToXml();
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
                // Mesajele nelivrate raman in coada persistenta.
            }
            catch (SocketException)
            {
                // Mesajele nelivrate raman in coada persistenta.
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

        string[] recipients = _subscriptions
            .Where(item => item.Value.Contains(packet.Topic))
            .Select(item => item.Key)
            .ToArray();

        if (recipients.Length == 0)
        {
            SendResponse(stream, false, "NO_SUBSCRIBERS", "Nu exista abonati pentru topicul indicat; mesajul nu a fost retinut.");
            return;
        }

        foreach (string clientId in recipients)
        {
            _pendingMessages.GetOrAdd(clientId, _ => new ConcurrentQueue<Message>()).Enqueue(message);
            _ = Task.Run(() => DeliverPendingMessagesForClient(clientId));
        }

        SaveToXml();
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

        // Un receiver se poate abona la mai multe topicuri deodata, separate prin virgula in campul Topic.
        HashSet<string> topics = ParseTopics(packet.Topic);
        if (topics.Count == 0)
        {
            SendResponse(stream, false, "INVALID_PACKET", "Cel putin un topic valid este obligatoriu pentru SUBSCRIBE.");
            return;
        }

        _subscriptions[packet.ClientId] = topics;
        _activeSockets[packet.ClientId] = client;
        _activeReaders[packet.ClientId] = reader;
        SaveToXml();
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

    /// <summary>
    /// Adauga unul sau mai multe topicuri la abonamentul deja existent al unui client, fara sa
    /// atinga conexiunea de livrare curenta (aceasta este o cerere scurta, de tip request/response,
    /// nu conexiunea persistenta creata la SUBSCRIBE). Astfel un receiver se poate conecta initial
    /// la un singur topic si poate cere ulterior sa fie abonat si la altele, fara sa se deconecteze.
    /// </summary>
    private void HandleAddTopic(Packet packet, NetworkStream stream)
    {
        if (string.IsNullOrWhiteSpace(packet.ClientId))
        {
            SendResponse(stream, false, "INVALID_SUBSCRIPTION", "ClientId este obligatoriu.");
            return;
        }

        if (!_subscriptions.ContainsKey(packet.ClientId))
        {
            SendResponse(stream, false, "UNKNOWN_CLIENT", "Clientul trebuie sa fie deja abonat (SUBSCRIBE) inainte de a adauga topicuri noi.");
            return;
        }

        HashSet<string> newTopics = ParseTopics(packet.Topic);
        if (newTopics.Count == 0)
        {
            SendResponse(stream, false, "INVALID_PACKET", "Cel putin un topic valid este obligatoriu.");
            return;
        }

        HashSet<string> topics = _subscriptions.AddOrUpdate(
            packet.ClientId,
            _ => newTopics,
            (_, existing) =>
            {
                var merged = new HashSet<string>(existing, StringComparer.OrdinalIgnoreCase);
                merged.UnionWith(newTopics);
                return merged;
            });

        SaveToXml();
        string joinedTopics = string.Join(", ", topics.OrderBy(topic => topic, StringComparer.OrdinalIgnoreCase));
        SendResponse(stream, true, "TOPIC_ADDED", $"Topicuri curente: {joinedTopics}.");
        WriteActivity("SUBSCRIBE", $"{packet.ClientId} a adaugat topicul(urile) „{string.Join(", ", newTopics)}”. Total abonamente: {joinedTopics}.");
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

            SaveToXml();
        }
        finally
        {
            deliveryLock.Release();
        }
    }

    /// <summary>
    /// Numara incercarile esuate pentru un mesaj/client. Dupa <see cref="MaxDeliveryAttempts"/> esecuri
    /// mesajul este scos din coada de livrare si mutat in dead-letter, ca sa nu blocheze la infinit
    /// coada clientului respectiv cu un mesaj nelivrabil.
    /// </summary>
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

    /// <summary>Descompune campul Topic al pachetului SUBSCRIBE intr-o multime de topicuri unice (separate prin virgula).</summary>
    private static HashSet<string> ParseTopics(string rawTopic)
    {
        var topics = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (string candidate in (rawTopic ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            topics.Add(candidate);
        }

        return topics;
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

    private void SaveToXml()
    {
        lock (_fileLock)
        {
            try
            {
                BrokerState state = new()
                {
                    Subscriptions = _subscriptions.Select(item => new SubscriptionItem
                    {
                        ClientId = item.Key,
                        Topics = item.Value.OrderBy(topic => topic, StringComparer.OrdinalIgnoreCase).ToList()
                    }).ToList(),
                    PendingMessages = _pendingMessages.SelectMany(queue => queue.Value.Select(message => new PendingMessageItem { ClientId = queue.Key, Message = message })).ToList(),
                    DeadLetterMessages = _deadLetterMessages.Values.Select(record => new DeadLetterItem
                    {
                        Id = record.Id,
                        ClientId = record.ClientId,
                        Message = record.Message,
                        Reason = record.Reason,
                        FailedAt = record.FailedAt,
                        Attempts = record.Attempts
                    }).ToList()
                };
                string temporaryPath = _xmlPath + ".tmp";
                XmlSerializer serializer = new(typeof(BrokerState));
                using (StreamWriter writer = new(temporaryPath, false, Utf8))
                {
                    serializer.Serialize(writer, state);
                }
                File.Move(temporaryPath, _xmlPath, true);
            }
            catch (Exception ex)
            {
                WriteActivity("ERROR", $"Salvarea starii a esuat: {ex.Message}");
            }
        }
    }

    private void LoadFromXml()
    {
        if (!File.Exists(_xmlPath))
        {
            return;
        }

        try
        {
            XmlSerializer serializer = new(typeof(BrokerState));
            using StreamReader reader = new(_xmlPath, Utf8);
            BrokerState? state = serializer.Deserialize(reader) as BrokerState;
            if (state is null)
            {
                return;
            }

            foreach (SubscriptionItem subscription in state.Subscriptions.Where(item => !string.IsNullOrWhiteSpace(item.ClientId)))
            {
                var topics = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (string topic in subscription.Topics.Where(topic => !string.IsNullOrWhiteSpace(topic)))
                {
                    topics.Add(topic.Trim());
                }
                // Compatibilitate cu fisiere vechi (dinainte de suportul multi-topic), care foloseau campul singular Topic.
                if (!string.IsNullOrWhiteSpace(subscription.Topic))
                {
                    topics.Add(subscription.Topic.Trim());
                }

                if (topics.Count > 0)
                {
                    _subscriptions[subscription.ClientId] = topics;
                }
            }
            foreach (PendingMessageItem item in state.PendingMessages.Where(item => !string.IsNullOrWhiteSpace(item.ClientId)))
            {
                _pendingMessages.GetOrAdd(item.ClientId, _ => new ConcurrentQueue<Message>()).Enqueue(item.Message);
            }
            foreach (DeadLetterItem item in state.DeadLetterMessages.Where(item => !string.IsNullOrWhiteSpace(item.ClientId)))
            {
                string id = string.IsNullOrWhiteSpace(item.Id) ? Guid.NewGuid().ToString() : item.Id;
                _deadLetterMessages[id] = new DeadLetterRecord
                {
                    Id = id,
                    ClientId = item.ClientId,
                    Message = item.Message,
                    Reason = item.Reason,
                    FailedAt = item.FailedAt,
                    Attempts = item.Attempts
                };
            }
            WriteActivity("STORAGE", $"Restaurate {state.Subscriptions.Count} abonamente, {state.PendingMessages.Count} mesaje pending si {state.DeadLetterMessages.Count} in dead-letter.");
        }
        catch (InvalidOperationException)
        {
            WriteActivity("ERROR", "Fisierul XML de stocare este invalid.");
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
