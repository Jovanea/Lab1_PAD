using System.Collections.ObjectModel;
using System.Windows.Input;
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
    private readonly ObservableCollection<DeadLetterRow> _deadLetters = new();
    private readonly DispatcherTimer _refreshTimer;

    public MainWindow()
    {
        InitializeComponent();
        PortBox.Text = EnvironmentSettings.BrokerPort.ToString();
        SubscribersList.ItemsSource = _subscribers;
        LogList.ItemsSource = _activities;
        DeadLetterList.ItemsSource = _deadLetters;
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
        OrphanCountText.Text = snapshot.OrphanCount.ToString();
        DeadLetterCountText.Text = snapshot.DeadLetterCount.ToString();
        _subscribers.Clear();
        foreach (BrokerSubscriber subscriber in snapshot.Subscribers)
        {
            _subscribers.Add(new SubscriberRow(subscriber, OnSimulateDeliveryFailure));
        }
        _deadLetters.Clear();
        foreach (BrokerDeadLetter entry in snapshot.DeadLetters)
        {
            _deadLetters.Add(new DeadLetterRow(entry, OnRequeueDeadLetter, OnDiscardDeadLetter));
        }
    }

    private void SetStatus(string text, IBrush color)
    {
        StatusText.Text = text;
        StatusDot.Fill = color;
    }

    private void OnRequeueDeadLetter(string id)
    {
        _server.RequeueDeadLetter(id);
        RefreshSnapshot();
    }

    private void OnDiscardDeadLetter(string id)
    {
        _server.DiscardDeadLetter(id);
        RefreshSnapshot();
    }

    private void OnClearDeadLettersClick(object? sender, RoutedEventArgs e)
    {
        _server.ClearDeadLetters();
        RefreshSnapshot();
    }

    private void OnClearSubscribersClick(object? sender, RoutedEventArgs e)
    {
        _server.ClearAllSubscribers();
        RefreshSnapshot();
    }

    private void OnSimulateDeliveryFailure(string clientId)
    {
        _server.SimulateDeliveryFailure(clientId);
        RefreshSnapshot();
    }

    private sealed class SubscriberRow
    {
        public SubscriberRow(BrokerSubscriber subscriber, Action<string> simulateFailure)
        {
            ClientId = subscriber.ClientId;
            Endpoint = subscriber.Endpoint;
            Topic = subscriber.Topic;
            State = subscriber.IsConnected ? "Conectat" : "Offline";
            StateColor = subscriber.IsConnected ? Brushes.SeaGreen : Brushes.Gray;
            SimulateFailureCommand = new RelayCommand(() => simulateFailure(subscriber.ClientId));
        }

        public string ClientId { get; }
        public string Endpoint { get; }
        public string Topic { get; }
        public string State { get; }
        public IBrush StateColor { get; }
        public ICommand SimulateFailureCommand { get; }
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
                "DEAD_LETTER" => Brushes.Crimson,
                "REQUEUE" => Brushes.DodgerBlue,
                _ => Brushes.SlateGray
            };
        }

        public string Time { get; }
        public string Category { get; }
        public string Detail { get; }
        public IBrush BadgeColor { get; }
    }

    private sealed class DeadLetterRow
    {
        public DeadLetterRow(BrokerDeadLetter entry, Action<string> requeue, Action<string> discard)
        {
            Id = entry.Id;
            ClientId = entry.ClientId;
            Topic = entry.Topic;
            Payload = entry.Payload;
            Reason = entry.Reason;
            Meta = $"{entry.FailedAt:HH:mm:ss} · incercari: {entry.Attempts}";
            RequeueCommand = new RelayCommand(() => requeue(entry.Id));
            DiscardCommand = new RelayCommand(() => discard(entry.Id));
        }

        public string Id { get; }
        public string ClientId { get; }
        public string Topic { get; }
        public string Payload { get; }
        public string Reason { get; }
        public string Meta { get; }
        public ICommand RequeueCommand { get; }
        public ICommand DiscardCommand { get; }
    }

    private sealed class RelayCommand : ICommand
    {
        private readonly Action _execute;

        public RelayCommand(Action execute) => _execute = execute;

        public event EventHandler? CanExecuteChanged { add { } remove { } }

        public bool CanExecute(object? parameter) => true;

        public void Execute(object? parameter) => _execute();
    }
}
