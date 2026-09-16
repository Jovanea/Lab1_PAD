using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Interactivity;

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

    public class SentEntry
    {
        public string Topic { get; set; } = string.Empty;
        public string Payload { get; set; } = string.Empty;
        public string Time { get; set; } = string.Empty;
        public string StatusIcon { get; set; } = string.Empty;
    }

    public partial class MainWindow : Window
    {
        private readonly ObservableCollection<SentEntry> _history = new();

        public MainWindow()
        {
            InitializeComponent();
            HistoryList.ItemsSource = _history;
        }

        private async void OnSendClick(object? sender, RoutedEventArgs e)
        {
            string topic = TopicBox.Text?.Trim() ?? string.Empty;
            string payload = MessageBox.Text?.Trim() ?? string.Empty;

            if (string.IsNullOrWhiteSpace(topic) || string.IsNullOrWhiteSpace(payload))
            {
                StatusText.Text = "Topic-ul și mesajul sunt obligatorii.";
                StatusText.Foreground = Avalonia.Media.Brushes.OrangeRed;
                return;
            }

            SendButton.IsEnabled = false;
            StatusText.Text = "Se trimite...";
            StatusText.Foreground = Avalonia.Media.Brushes.Gray;

            bool success = await Task.Run(() => SendMessage(topic, payload));

            _history.Insert(0, new SentEntry
            {
                Topic = topic,
                Payload = payload,
                Time = DateTime.Now.ToString("HH:mm:ss"),
                StatusIcon = success ? "✅" : "❌"
            });

            StatusText.Text = success ? "Mesaj expediat către Broker." : "Eroare de conexiune la Broker.";
            StatusText.Foreground = success ? Avalonia.Media.Brushes.SeaGreen : Avalonia.Media.Brushes.OrangeRed;

            if (success)
            {
                MessageBox.Text = string.Empty;
            }

            SendButton.IsEnabled = true;
        }

        private static bool SendMessage(string topic, string payload)
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
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}
