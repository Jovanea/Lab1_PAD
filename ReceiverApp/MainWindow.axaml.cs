using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Media;
using Avalonia.Threading;
using MessageHub.Configuration;

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

    public class ReceivedEntry
    {
        public string Topic { get; set; } = string.Empty;
        public string Payload { get; set; } = string.Empty;
        public string Time { get; set; } = string.Empty;
    }

    public class BrokerResponse
    {
        public bool Success { get; set; }
        public string Code { get; set; } = string.Empty;
        public string Detail { get; set; } = string.Empty;
    }

    public partial class MainWindow : Window
    {
        private readonly ObservableCollection<ReceivedEntry> _feed = new();

        public MainWindow()
        {
            InitializeComponent();
            BrokerHostBox.Text = EnvironmentSettings.BrokerHost;
            FeedList.ItemsSource = _feed;
        }

        private void OnConnectClick(object? sender, RoutedEventArgs e)
        {
            string brokerHost = BrokerHostBox.Text?.Trim() ?? string.Empty;
            string clientId = ClientIdBox.Text?.Trim() ?? string.Empty;
            string topic = TopicBox.Text?.Trim() ?? string.Empty;

            if (string.IsNullOrWhiteSpace(brokerHost) || string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(topic))
            {
                SetStatus("Adresa brokerului, Client ID și Topic sunt obligatorii.", Brushes.OrangeRed);
                return;
            }

            ConnectButton.IsEnabled = false;
            ClientIdBox.IsEnabled = false;
            TopicBox.IsEnabled = false;
            SetStatus("Se conectează...", Brushes.Gray);

            Task.Run(() => ConnectAndListen(brokerHost, clientId, topic));
        }

        private void ConnectAndListen(string brokerHost, string clientId, string topic)
        {
            try
            {
                using TcpClient client = new TcpClient(brokerHost, EnvironmentSettings.BrokerPort);
                using NetworkStream stream = client.GetStream();
                using StreamWriter writer = new StreamWriter(stream, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
                using StreamReader reader = new StreamReader(stream, new UTF8Encoding(false), leaveOpen: true);

                var packet = new Packet { Action = "SUBSCRIBE", Topic = topic, ClientId = clientId };
                writer.WriteLine(JsonSerializer.Serialize(packet));

                stream.ReadTimeout = 5000;
                BrokerResponse? response = JsonSerializer.Deserialize<BrokerResponse>(reader.ReadLine() ?? string.Empty);
                if (response?.Success != true)
                {
                    throw new InvalidOperationException(response?.Detail ?? "Brokerul nu a confirmat abonarea.");
                }
                stream.ReadTimeout = Timeout.Infinite;

                Dispatcher.UIThread.Post(() =>
                    SetStatus($"Conectat ca '{clientId}' pe topicul '{topic}'", Brushes.SeaGreen));

                while (true)
                {
                    string? rawData = reader.ReadLine();
                    if (rawData == null)
                    {
                        Dispatcher.UIThread.Post(() =>
                            SetStatus("Conexiunea cu brokerul s-a închis.", Brushes.OrangeRed));
                        break;
                    }

                    try
                    {
                        var msg = JsonSerializer.Deserialize<Message>(rawData);
                        if (msg != null)
                        {
                            // Confirmăm către broker că mesajul a fost efectiv primit,
                            // altfel brokerul nu poate ști dacă a fost livrat cu adevărat.
                            writer.WriteLine($"ACK:{msg.Id}");

                            Dispatcher.UIThread.Post(() =>
                            {
                                _feed.Insert(0, new ReceivedEntry
                                {
                                    Topic = msg.Topic,
                                    Payload = msg.Payload,
                                    Time = msg.Timestamp.ToLocalTime().ToString("HH:mm:ss")
                                });
                            });
                        }
                    }
                    catch { }
                }
            }
            catch (Exception ex)
            {
                Dispatcher.UIThread.Post(() =>
                    SetStatus($"Eroare de conexiune: {ex.Message}", Brushes.OrangeRed));
            }

            Dispatcher.UIThread.Post(() =>
            {
                ConnectButton.IsEnabled = true;
                ClientIdBox.IsEnabled = true;
                TopicBox.IsEnabled = true;
            });
        }

        private void SetStatus(string text, IBrush color)
        {
            StatusText.Text = text;
            StatusText.Foreground = color;
            StatusDot.Fill = color;
        }
    }
}
