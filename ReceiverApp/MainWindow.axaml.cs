using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
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
        private readonly HashSet<string> _subscribedTopics = new(StringComparer.OrdinalIgnoreCase);
        private string _brokerHost = string.Empty;
        private int _brokerPort;
        private string _clientId = string.Empty;

        public MainWindow()
        {
            InitializeComponent();
            FeedList.ItemsSource = _feed;
            BrokerAddressBox.Text = $"{EnvironmentSettings.BrokerHost}:{EnvironmentSettings.BrokerPort}";
        }

        private void OnConnectClick(object? sender, RoutedEventArgs e)
        {
            string clientId = ClientIdBox.Text?.Trim() ?? string.Empty;
            string topic = TopicBox.Text?.Trim() ?? string.Empty;

            if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(topic))
            {
                SetStatus("Client ID și Topic sunt obligatorii.", Brushes.OrangeRed);
                return;
            }

            if (!EnvironmentSettings.TryParseBrokerAddress(BrokerAddressBox.Text, out string brokerHost, out int brokerPort))
            {
                SetStatus("Adresa brokerului este invalida (ex: 192.168.1.10:5000).", Brushes.OrangeRed);
                return;
            }

            _brokerHost = brokerHost;
            _brokerPort = brokerPort;
            _clientId = clientId;

            ConnectButton.IsEnabled = false;
            BrokerAddressBox.IsEnabled = false;
            ClientIdBox.IsEnabled = false;
            TopicBox.IsEnabled = false;
            SetStatus("Se conectează...", Brushes.Gray);

            Task.Run(() => ConnectAndListen(brokerHost, brokerPort, clientId, topic));
        }

        private void OnAddTopicClick(object? sender, RoutedEventArgs e)
        {
            string newTopic = NewTopicBox.Text?.Trim() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(newTopic))
            {
                return;
            }

            AddTopicButton.IsEnabled = false;
            Task.Run(() => SendAddTopicRequest(newTopic));
        }

        private void SendAddTopicRequest(string newTopic)
        {
            try
            {
                using TcpClient client = EnvironmentSettings.ConnectToBroker(_brokerHost, _brokerPort);
                using NetworkStream stream = client.GetStream();
                using StreamWriter writer = new StreamWriter(stream, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
                using StreamReader reader = new StreamReader(stream, new UTF8Encoding(false), leaveOpen: true);

                var packet = new Packet { Action = "ADD_TOPIC", Topic = newTopic, ClientId = _clientId };
                writer.WriteLine(JsonSerializer.Serialize(packet));

                stream.ReadTimeout = 5000;
                BrokerResponse? response = JsonSerializer.Deserialize<BrokerResponse>(reader.ReadLine() ?? string.Empty);

                Dispatcher.UIThread.Post(() =>
                {
                    if (response?.Success == true)
                    {
                        _subscribedTopics.Add(newTopic);
                        NewTopicBox.Text = string.Empty;
                        SetStatus(response.Detail, Brushes.SeaGreen);
                        RefreshTopicsText();
                    }
                    else
                    {
                        SetStatus(response?.Detail ?? "Adaugarea topicului a eșuat.", Brushes.OrangeRed);
                    }
                    AddTopicButton.IsEnabled = true;
                });
            }
            catch (Exception ex)
            {
                Dispatcher.UIThread.Post(() =>
                {
                    SetStatus($"Eroare la adaugarea topicului: {ex.Message}", Brushes.OrangeRed);
                    AddTopicButton.IsEnabled = true;
                });
            }
        }

        private void RefreshTopicsText()
        {
            TopicsText.Text = _subscribedTopics.Count == 0
                ? string.Empty
                : $"Abonat la: {string.Join(", ", _subscribedTopics.OrderBy(t => t, StringComparer.OrdinalIgnoreCase))}";
        }

        private void ConnectAndListen(string brokerHost, int brokerPort, string clientId, string topic)
        {
            try
            {
                using TcpClient client = EnvironmentSettings.ConnectToBroker(brokerHost, brokerPort);
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

                // portul local este alocat de sistemul de operare, deci fiecare receiver are propriul port
                string local = client.Client.LocalEndPoint?.ToString() ?? "-";
                string remote = client.Client.RemoteEndPoint?.ToString() ?? "-";

                Dispatcher.UIThread.Post(() =>
                {
                    ConnectionText.Text = $"Adresa locala: {local}  →  Broker: {remote}";
                    _subscribedTopics.Add(topic);
                    NewTopicBox.IsEnabled = true;
                    AddTopicButton.IsEnabled = true;
                    RefreshTopicsText();
                    SetStatus($"Conectat ca '{clientId}'. {response.Detail}", Brushes.SeaGreen);
                });

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
            catch (InvalidOperationException ex)
            {
                Dispatcher.UIThread.Post(() =>
                    SetStatus($"Abonare respinsa: {ex.Message}", Brushes.OrangeRed));
            }
            catch (Exception ex)
            {
                Dispatcher.UIThread.Post(() =>
                    SetStatus($"Eroare de conexiune: {ex.Message}", Brushes.OrangeRed));
            }

            Dispatcher.UIThread.Post(() =>
            {
                ConnectButton.IsEnabled = true;
                BrokerAddressBox.IsEnabled = true;
                ClientIdBox.IsEnabled = true;
                TopicBox.IsEnabled = true;
                NewTopicBox.IsEnabled = false;
                AddTopicButton.IsEnabled = false;
                ConnectionText.Text = string.Empty;
                _subscribedTopics.Clear();
                RefreshTopicsText();
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
