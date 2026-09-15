using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace ReceiverApp
{
    public class Message
    {
        public string Id { get; set; } = string.Empty;
        public string Topic { get; set; } = string.Empty;
        public string Payload { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
    }

    public class Packet
    {
        public string Action { get; set; } = "SUBSCRIBE";
        public string Topic { get; set; } = string.Empty;
        public string ClientId { get; set; } = string.Empty;
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== RECEIVER (SUBSCRIBER - OFFLINE SUPPORT) ===");
            
            Console.Write("Introduceți ID-ul unic al clientului (ex: receiver_1): ");
            string? clientId = Console.ReadLine();
            while (string.IsNullOrWhiteSpace(clientId))
            {
                Console.Write("ID-ul nu poate fi gol. Reintroduceți: ");
                clientId = Console.ReadLine();
            }

            Console.Write("Introduceți Topic-ul la care vă abonați (ex: Curs): ");
            string? topic = Console.ReadLine();
            while (string.IsNullOrWhiteSpace(topic))
            {
                Console.Write("Topic invalid. Reintroduceți: ");
                topic = Console.ReadLine();
            }

            ConnectAndListen(clientId, topic);
        }

        private static void ConnectAndListen(string clientId, string topic)
        {
            try
            {
                TcpClient client = new TcpClient("127.0.0.1", 5000);
                NetworkStream stream = client.GetStream();
                StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
                StreamReader reader = new StreamReader(stream, Encoding.UTF8);

                // Cerere de abonare cu identificator de client
                var packet = new Packet { Action = "SUBSCRIBE", Topic = topic, ClientId = clientId };
                writer.WriteLine(JsonSerializer.Serialize(packet));

                Console.WriteLine($"\n[OK] Conectat ca '{clientId}' pe topicul '{topic}'. Se preiau mesajele (inclusiv cele istorice/offline)...\n");

                while (true)
                {
                    string? rawData = reader.ReadLine();
                    if (rawData == null)
                    {
                        // Broker-ul a închis conexiunea
                        Console.WriteLine("[DECONECTAT] Conexiunea cu brokerul s-a închis.");
                        break;
                    }

                    try
                    {
                        var msg = JsonSerializer.Deserialize<Message>(rawData);
                        if (msg != null)
                        {
                            Console.WriteLine($"[{msg.Timestamp:HH:mm:ss}] Mesaj primit on/offline [{msg.Topic}]: {msg.Payload}");
                            // Confirmăm către broker că mesajul a fost efectiv primit și procesat,
                            // altfel brokerul nu poate ști dacă a fost livrat cu adevărat.
                            writer.WriteLine($"ACK:{msg.Id}");
                        }
                    }
                    catch { }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DECONECTAT / EROARE]: {ex.Message}");
            }
        }
    }
}