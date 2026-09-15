using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace BrokerApp
{
    public class Message
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string Topic { get; set; } = string.Empty;
        public string Payload { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    }

    public class Packet
    {
        public string Action { get; set; } = string.Empty; // "PUBLISH" sau "SUBSCRIBE"
        public string Topic { get; set; } = string.Empty;
        public string ClientId { get; set; } = string.Empty; // Identificator unic al receiver-ului
        public Message? MessageData { get; set; }
    }

    public class SubscriberInfo
    {
        public string ClientId { get; set; } = string.Empty;
        public string Topic { get; set; } = string.Empty;
        [XmlIgnore]
        public TcpClient? ClientSocket { get; set; }
    }

    // Clasă DTO pentru persistența XML
    public class PendingMessageItem
    {
        public string ClientId { get; set; } = string.Empty;
        public Message Message { get; set; } = new Message();
    }

    class Program
    {
        // Stochează abonații cunoscuți (ClientId -> Topic)
        private static readonly ConcurrentDictionary<string, string> SubscribedClients = new();
        
        // Cozi de mesaje în așteptare pentru FIECARE ClientId: ClientId -> Queue<Message>
        private static readonly ConcurrentDictionary<string, ConcurrentQueue<Message>> PendingMessages = new();

        // Socket-urile active ale clienților conectați în prezent: ClientId -> TcpClient
        private static readonly ConcurrentDictionary<string, TcpClient> ActiveSockets = new();

        private static readonly object FileLock = new();
        private static readonly string XmlPath = "broker_storage.xml";

        static void Main(string[] args)
        {
            // Încărcăm mesajele offline netrimise din XML la pornirea brokerului
            LoadFromXml();

            int port = 5000;
            TcpListener listener = new TcpListener(IPAddress.Any, port);
            listener.Start();

            Console.WriteLine($"=== BROKER PORNIT PE PORTUL {port} ===");
            Console.WriteLine("[INFO] Sistemul de persistență Offline (Store & Forward) este Activ.\n");

            while (true)
            {
                try
                {
                    TcpClient client = listener.AcceptTcpClient();
                    // Handling concurent per client
                    Task.Run(() => HandleClient(client));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[EROARE LISTENER]: {ex.Message}");
                }
            }
        }

        private static void HandleClient(TcpClient client)
        {
            NetworkStream stream = client.GetStream();
            StreamReader reader = new StreamReader(stream, Encoding.UTF8);

            try
            {
                string? rawData = reader.ReadLine();
                if (string.IsNullOrWhiteSpace(rawData)) return;

                Packet? packet = null;
                try
                {
                    packet = JsonSerializer.Deserialize<Packet>(rawData);
                }
                catch
                {
                    Console.WriteLine("[WARN] Mesaj invalid primit. Ignorat.");
                    return;
                }

                if (packet == null || string.IsNullOrWhiteSpace(packet.Topic)) return;

                // 1. CAZUL PUBLISH (Trimitere mesaj de la Sender)
                if (packet.Action == "PUBLISH" && packet.MessageData != null)
                {
                    if (string.IsNullOrWhiteSpace(packet.MessageData.Payload)) return;

                    Console.WriteLine($"\n[PUBLISH] Topic: '{packet.Topic}' | Mesaj: '{packet.MessageData.Payload}'");

                    // Adăugăm mesajul în coada TUTUROR clienților abonați la acest topic[cite: 1]
                    var targetClients = SubscribedClients.Where(x => x.Value.Equals(packet.Topic, StringComparison.OrdinalIgnoreCase)).Select(x => x.Key);

                    foreach (var clientId in targetClients)
                    {
                        var queue = PendingMessages.GetOrAdd(clientId, _ => new ConcurrentQueue<Message>());
                        queue.Enqueue(packet.MessageData);

                        // Dacă clientul este conectat ACUM, îi trimitem mesajul imediat
                        DeliverPendingMessagesForClient(clientId);
                    }

                    SaveToXml();
                }
                // 2. CAZUL SUBSCRIBE (Abonare / Reconectare Receiver)[cite: 1]
                else if (packet.Action == "SUBSCRIBE" && !string.IsNullOrWhiteSpace(packet.ClientId))
                {
                    string clientId = packet.ClientId;
                    string topic = packet.Topic;

                    // Salvăm abonamentul clientului
                    SubscribedClients[clientId] = topic;
                    ActiveSockets[clientId] = client;

                    Console.WriteLine($"[SUBSCRIBE] Receiver '{clientId}' s-a conectat/abonat la topicul '{topic}'");

                    // Îi livrăm TOATE mesajele restante adunate cât timp a fost offline[cite: 1]
                    DeliverPendingMessagesForClient(clientId);

                    // Menținem conexiunea deschisă și detectăm când se deconectează
                    while (client.Connected)
                    {
                        Thread.Sleep(1000);
                    }

                    // Când se deconectează, îl scoatem doar din socket-uri active (abonamentul și mesajele offline RĂMÂN)[cite: 1]
                    ActiveSockets.TryRemove(clientId, out _);
                    Console.WriteLine($"[OFFLINE] Receiver '{clientId}' s-a deconectat. Mesajele viitoare vor fi salvate pe disk.");
                }
            }
            catch (Exception)
            {
                // Conexiunea s-a întrerupt
            }
        }

        // Trite toate mesajele restante din coada unui client specific[cite: 1]
        private static void DeliverPendingMessagesForClient(string clientId)
        {
            if (!ActiveSockets.TryGetValue(clientId, out var client) || !client.Connected)
                return;

            if (PendingMessages.TryGetValue(clientId, out var queue))
            {
                while (queue.TryPeek(out var message))
                {
                    try
                    {
                        NetworkStream ns = client.GetStream();
                        string jsonString = JsonSerializer.Serialize(message) + "\n";
                        byte[] data = Encoding.UTF8.GetBytes(jsonString);

                        ns.Write(data, 0, data.Length);
                        ns.Flush();

                        // Dacă s-a trimis cu succes pe socket, îl scoatem din coadă[cite: 1]
                        queue.TryDequeue(out _);
                        Console.WriteLine($"  -> Livrat mesaj istoric/nou către '{clientId}' [{message.Topic}]: {message.Payload}");
                    }
                    catch
                    {
                        // Socket-ul a căzut în timpul trimiterii, oprim livrarea (mesajul rămâne în coadă pentru data viitoare)[cite: 1]
                        ActiveSockets.TryRemove(clientId, out _);
                        break;
                    }
                }
                SaveToXml();
            }
        }

        // Persistență XML - Salvare stare pe disk[cite: 1]
        private static void SaveToXml()
        {
            lock (FileLock)
            {
                try
                {
                    var list = new List<PendingMessageItem>();
                    foreach (var kvp in PendingMessages)
                    {
                        foreach (var msg in kvp.Value)
                        {
                            list.Add(new PendingMessageItem { ClientId = kvp.Key, Message = msg });
                        }
                    }

                    XmlSerializer serializer = new XmlSerializer(typeof(List<PendingMessageItem>));
                    using StreamWriter writer = new StreamWriter(XmlPath);
                    serializer.Serialize(writer, list);
                }
                catch { }
            }
        }

        // Încărcare din XML la repornirea Brokerului[cite: 1]
        private static void LoadFromXml()
        {
            if (!File.Exists(XmlPath)) return;

            lock (FileLock)
            {
                try
                {
                    XmlSerializer serializer = new XmlSerializer(typeof(List<PendingMessageItem>));
                    using StreamReader reader = new StreamReader(XmlPath);
                    var list = (List<PendingMessageItem>?)serializer.Deserialize(reader);

                    if (list != null)
                    {
                        foreach (var item in list)
                        {
                            var queue = PendingMessages.GetOrAdd(item.ClientId, _ => new ConcurrentQueue<Message>());
                            queue.Enqueue(item.Message);
                        }
                        Console.WriteLine($"[STORAGE] S-au încărcat din XML {list.Count} mesaje restante nepredate.");
                    }
                }
                catch { }
            }
        }
    }
}