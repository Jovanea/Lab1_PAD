using System.Collections.ObjectModel;
using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Media;
using Avalonia.Threading;
using MessageHub.Configuration;

namespace BrokerApp;

public partial class MainWindow : Window
{
    private readonly BrokerServer _server = new();
    private readonly ObservableCollection<SubscriberRow> _subscribers = new();
    private readonly ObservableCollection<ActivityRow> _activities = new();
    private readonly DispatcherTimer _refreshTimer;

    public MainWindow()
    {
        InitializeComponent();
        PortBox.Text = EnvironmentSettings.BrokerPort.ToString();
        SubscribersList.ItemsSource = _subscribers;
        LogList.ItemsSource = _activities;
        _server.Activity += OnBrokerActivity;
        _refreshTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(1) };
        _refreshTimer.Tick += (_, _) => RefreshSnapshot();
        _refreshTimer.Start();
        Opened += async (_, _) => await StartBrokerAsync();
        Closed += async (_, _) =>
        {
            _refreshTimer.Stop();
            await _server.StopAsync();
            _server.Dispose();
        };
    }

    private async void OnStartClick(object? sender, RoutedEventArgs e)
    {
        await StartBrokerAsync();
    }

    private async Task StartBrokerAsync()
    {
        if (!int.TryParse(PortBox.Text, out int port) || port is < 1 or > 65535)
        {
            SetStatus("Port invalid", Brushes.OrangeRed);
            return;
        }

        try
        {
            await _server.StartAsync(port);
            PortBox.IsEnabled = false;
            StartButton.IsEnabled = false;
            StopButton.IsEnabled = true;
            SetStatus("Broker activ", Brushes.SeaGreen);
            RefreshSnapshot();
        }
        catch (Exception ex)
        {
            SetStatus($"Eroare: {ex.Message}", Brushes.OrangeRed);
        }
    }

    private async void OnStopClick(object? sender, RoutedEventArgs e)
    {
        await _server.StopAsync();
        PortBox.IsEnabled = true;
        StartButton.IsEnabled = true;
        StopButton.IsEnabled = false;
        SetStatus("Oprit", Brushes.Gray);
        RefreshSnapshot();
    }

    private void OnBrokerActivity(object? sender, BrokerActivity activity)
    {
        Dispatcher.UIThread.Post(() =>
        {
            _activities.Insert(0, new ActivityRow(activity));
            if (_activities.Count > 120)
            {
                _activities.RemoveAt(_activities.Count - 1);
            }
            RefreshSnapshot();
        });
    }

    private void RefreshSnapshot()
    {
        BrokerSnapshot snapshot = _server.GetSnapshot();
        SubscriberCountText.Text = snapshot.SubscriberCount.ToString();
        ConnectedCountText.Text = snapshot.ConnectedCount.ToString();
        PendingCountText.Text = snapshot.PendingCount.ToString();
        _subscribers.Clear();
        foreach (BrokerSubscriber subscriber in snapshot.Subscribers)
        {
            _subscribers.Add(new SubscriberRow(subscriber));
        }
    }

    private void SetStatus(string text, IBrush color)
    {
        StatusText.Text = text;
        StatusDot.Fill = color;
    }

    private sealed class SubscriberRow
    {
        public SubscriberRow(BrokerSubscriber subscriber)
        {
            ClientId = subscriber.ClientId;
            Topic = subscriber.Topic;
            State = subscriber.IsConnected ? "Conectat" : "Offline";
            StateColor = subscriber.IsConnected ? Brushes.SeaGreen : Brushes.Gray;
        }

        public string ClientId { get; }
        public string Topic { get; }
        public string State { get; }
        public IBrush StateColor { get; }
    }

    private sealed class ActivityRow
    {
        public ActivityRow(BrokerActivity activity)
        {
            Time = activity.Time.ToString("HH:mm:ss");
            Category = activity.Category;
            Detail = activity.Detail;
            BadgeColor = activity.Category switch
            {
                "ERROR" => Brushes.OrangeRed,
                "DELIVERED" => Brushes.SeaGreen,
                "PUBLISH" => Brushes.DodgerBlue,
                "SUBSCRIBE" => Brushes.MediumPurple,
                "WAITING" => Brushes.DarkOrange,
                _ => Brushes.SlateGray
            };
        }

        public string Time { get; }
        public string Category { get; }
        public string Detail { get; }
        public IBrush BadgeColor { get; }
    }
}
