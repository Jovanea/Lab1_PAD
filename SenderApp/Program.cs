using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace SenderApp
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
        public string Action { get; set; } = "PUBLISH";
        public string Topic { get; set; } = string.Empty;
        public Message? MessageData { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== SENDER (PUBLISHER) ===");

            while (true)
            {
                Console.Write("\nTopic (ex: Curs) sau 'exit': ");
                string? topic = Console.ReadLine();
                if (topic == "exit") break;

                Console.Write("Mesaj: ");
                string? payload = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(topic) || string.IsNullOrWhiteSpace(payload))
                {
                    Console.WriteLine("Topic-ul și mesajul sunt obligatorii!");
                    continue;
                }

                SendMessage(topic, payload);
            }
        }

        private static void SendMessage(string topic, string payload)
        {
            try
            {
                using TcpClient client = new TcpClient("127.0.0.1", 5000);
                using NetworkStream stream = client.GetStream();
                using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

                var packet = new Packet
                {
                    Action = "PUBLISH",
                    Topic = topic,
                    MessageData = new Message { Topic = topic, Payload = payload }
                };

                writer.WriteLine(JsonSerializer.Serialize(packet));
                Console.WriteLine("-> [SUCCES] Mesaj expediat către Broker!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"-> [EROARE CONEXIUNE]: {ex.Message}");
            }
        }
    }
}