using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Interactivity;
using MessageHub.Configuration;

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

    public class BrokerResponse
    {
        public bool Success { get; set; }
        public string Code { get; set; } = string.Empty;
        public string Detail { get; set; } = string.Empty;
    }

    public partial class MainWindow : Window
    {
        private readonly ObservableCollection<SentEntry> _history = new();

        public MainWindow()
        {
            InitializeComponent();
            BrokerHostBox.Text = EnvironmentSettings.BrokerHost;
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

            string brokerHost = BrokerHostBox.Text?.Trim() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(brokerHost))
            {
                StatusText.Text = "Adresa brokerului este obligatorie.";
                StatusText.Foreground = Avalonia.Media.Brushes.OrangeRed;
                SendButton.IsEnabled = true;
                return;
            }

            (bool success, string detail) = await Task.Run(() => SendMessage(brokerHost, topic, payload));

            _history.Insert(0, new SentEntry
            {
                Topic = topic,
                Payload = payload,
                Time = DateTime.Now.ToString("HH:mm:ss"),
                StatusIcon = success ? "✅" : "❌"
            });

            StatusText.Text = success ? "Mesaj expediat către Broker." : "Eroare de conexiune la Broker.";
            StatusText.Text = detail;
            StatusText.Foreground = success ? Avalonia.Media.Brushes.SeaGreen : Avalonia.Media.Brushes.OrangeRed;

            if (success)
            {
                MessageBox.Text = string.Empty;
            }

            SendButton.IsEnabled = true;
        }

        private static (bool Success, string Detail) SendMessage(string brokerHost, string topic, string payload)
        {
            try
            {
                using TcpClient client = new TcpClient(brokerHost, EnvironmentSettings.BrokerPort);
                using NetworkStream stream = client.GetStream();
                using StreamWriter writer = new StreamWriter(stream, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
                using StreamReader reader = new StreamReader(stream, new UTF8Encoding(false), leaveOpen: true);

                var packet = new Packet
                {
                    Action = "PUBLISH",
                    Topic = topic,
                    MessageData = new Message { Topic = topic, Payload = payload }
                };

                writer.WriteLine(JsonSerializer.Serialize(packet));
                stream.ReadTimeout = 5000;
                BrokerResponse? response = JsonSerializer.Deserialize<BrokerResponse>(reader.ReadLine() ?? string.Empty);
                return response?.Success == true
                    ? (true, response.Detail)
                    : (false, response?.Detail ?? "Brokerul nu a confirmat mesajul.");
            }
            catch (Exception ex)
            {
                return (false, $"Eroare de conexiune la Broker: {ex.Message}");
            }
        }
    }
}
