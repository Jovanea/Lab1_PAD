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

public class SubscriptionItem
{
    public string ClientId { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
}

public class PendingMessageItem
{
    public string ClientId { get; set; } = string.Empty;
    public Message Message { get; set; } = new();
}

[XmlRoot("BrokerState")]
public class BrokerState
{
    public List<SubscriptionItem> Subscriptions { get; set; } = new();
    public List<PendingMessageItem> PendingMessages { get; set; } = new();
}

public record BrokerActivity(string Category, string Detail, DateTime Time);
public record BrokerSubscriber(string ClientId, string Topic, bool IsConnected);
public record BrokerSnapshot(int SubscriberCount, int ConnectedCount, int PendingCount, IReadOnlyList<BrokerSubscriber> Subscribers);

public sealed class BrokerServer : IDisposable
{
    private static readonly Encoding Utf8 = new UTF8Encoding(false);
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private readonly ConcurrentDictionary<string, string> _subscriptions = new();
    private readonly ConcurrentDictionary<string, ConcurrentQueue<Message>> _pendingMessages = new();
    private readonly ConcurrentDictionary<string, TcpClient> _activeSockets = new();
    private readonly ConcurrentDictionary<string, StreamReader> _activeReaders = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _deliveryLocks = new();
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
            .Select(item => new BrokerSubscriber(item.Key, item.Value, _activeSockets.ContainsKey(item.Key)))
            .ToList();

        return new BrokerSnapshot(
            subscribers.Count,
            _activeSockets.Count,
            _pendingMessages.Sum(item => item.Value.Count),
            subscribers);
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

                if (packet is null || string.IsNullOrWhiteSpace(packet.Action) || string.IsNullOrWhiteSpace(packet.Topic))
                {
                    SendResponse(stream, false, "INVALID_PACKET", "Action si Topic sunt obligatorii.");
                    return;
                }

                switch (packet.Action.ToUpperInvariant())
                {
                    case "PUBLISH":
                        HandlePublish(packet, stream);
                        return;
                    case "SUBSCRIBE":
                        HandleSubscribe(packet, client, stream, reader, cancellationToken);
                        return;
                    default:
                        SendResponse(stream, false, "UNKNOWN_ACTION", "Action acceptat: PUBLISH sau SUBSCRIBE.");
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
            .Where(item => string.Equals(item.Value, packet.Topic, StringComparison.OrdinalIgnoreCase))
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

    private void HandleSubscribe(Packet packet, TcpClient client, NetworkStream stream, StreamReader reader, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(packet.ClientId))
        {
            SendResponse(stream, false, "INVALID_SUBSCRIPTION", "ClientId este obligatoriu.");
            return;
        }

        _subscriptions[packet.ClientId] = packet.Topic;
        _activeSockets[packet.ClientId] = client;
        _activeReaders[packet.ClientId] = reader;
        SaveToXml();
        SendResponse(stream, true, "SUBSCRIBED", $"Abonat la topicul '{packet.Topic}'.");
        WriteActivity("SUBSCRIBE", $"{packet.ClientId} este abonat la „{packet.Topic}”.");
        DeliverPendingMessagesForClient(packet.ClientId);

        while (!cancellationToken.IsCancellationRequested && client.Connected)
        {
            Thread.Sleep(500);
        }

        RemoveActiveConnection(packet.ClientId, client);
    }

    private void DeliverPendingMessagesForClient(string clientId)
    {
        SemaphoreSlim deliveryLock = _deliveryLocks.GetOrAdd(clientId, _ => new SemaphoreSlim(1, 1));
        deliveryLock.Wait();
        try
        {
            if (!_activeSockets.TryGetValue(clientId, out TcpClient? client) || !client.Connected ||
                !_activeReaders.TryGetValue(clientId, out StreamReader? ackReader) ||
                !_pendingMessages.TryGetValue(clientId, out ConcurrentQueue<Message>? queue))
            {
                return;
            }

            while (queue.TryPeek(out Message? message))
            {
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
                        WriteActivity("WAITING", $"Livrarea catre {clientId} va fi reincercata.");
                        break;
                    }

                    queue.TryDequeue(out _);
                    WriteActivity("DELIVERED", $"Mesaj livrat si confirmat de {clientId}.");
                }
                catch (IOException)
                {
                    RemoveActiveConnection(clientId, client);
                    WriteActivity("WAITING", $"{clientId} nu raspunde; mesajul ramane pending.");
                    break;
                }
                catch (SocketException)
                {
                    RemoveActiveConnection(clientId, client);
                    WriteActivity("WAITING", $"{clientId} nu raspunde; mesajul ramane pending.");
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
                    Subscriptions = _subscriptions.Select(item => new SubscriptionItem { ClientId = item.Key, Topic = item.Value }).ToList(),
                    PendingMessages = _pendingMessages.SelectMany(queue => queue.Value.Select(message => new PendingMessageItem { ClientId = queue.Key, Message = message })).ToList()
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

            foreach (SubscriptionItem subscription in state.Subscriptions.Where(item => !string.IsNullOrWhiteSpace(item.ClientId) && !string.IsNullOrWhiteSpace(item.Topic)))
            {
                _subscriptions[subscription.ClientId] = subscription.Topic;
            }
            foreach (PendingMessageItem item in state.PendingMessages.Where(item => !string.IsNullOrWhiteSpace(item.ClientId)))
            {
                _pendingMessages.GetOrAdd(item.ClientId, _ => new ConcurrentQueue<Message>()).Enqueue(item.Message);
            }
            WriteActivity("STORAGE", $"Restaurate {state.Subscriptions.Count} abonamente si {state.PendingMessages.Count} mesaje.");
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
