using System.Globalization;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace MessageHub.Configuration;

public static class EnvironmentSettings
{
    public static string BrokerHost => GetValue("BROKER_HOST", "127.0.0.1");

    public static int BrokerPort => GetIntValue("BROKER_PORT", 5000);

    // adresele IPv4 ale placilor de retea active, pe care le pot folosi clientii de pe alte calculatoare
    public static string[] GetLocalIPv4Addresses()
    {
        return NetworkInterface.GetAllNetworkInterfaces()
            .Where(nic => nic.OperationalStatus == OperationalStatus.Up && nic.NetworkInterfaceType != NetworkInterfaceType.Loopback)
            .SelectMany(nic => nic.GetIPProperties().UnicastAddresses)
            .Select(address => address.Address)
            .Where(address => address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(address))
            .Select(address => address.ToString())
            .Distinct()
            .ToArray();
    }

    // accepta "host" sau "host:port"; daca portul lipseste se foloseste BROKER_PORT
    public static bool TryParseBrokerAddress(string? text, out string host, out int port)
    {
        host = string.Empty;
        port = BrokerPort;
        string value = text?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        int separator = value.LastIndexOf(':');
        if (separator > 0)
        {
            if (!int.TryParse(value[(separator + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out port) || port is < 1 or > 65535)
            {
                return false;
            }

            value = value[..separator];
        }

        host = value.Trim();
        return host.Length > 0;
    }

    // conectare cu timeout: un IP gresit din LAN nu mai blocheaza clientul ~20 de secunde
    public static TcpClient ConnectToBroker(string host, int port, int timeoutMilliseconds = 3000)
    {
        var client = new TcpClient();
        try
        {
            Task connect = client.ConnectAsync(host, port);
            if (Task.WhenAny(connect, Task.Delay(timeoutMilliseconds)).GetAwaiter().GetResult() != connect)
            {
                throw new IOException($"Brokerul {host}:{port} nu raspunde (verificati IP-ul si firewall-ul).");
            }

            connect.GetAwaiter().GetResult();
            return client;
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    public static string FindProjectRoot()
    {
        string? current = AppContext.BaseDirectory;
        while (!string.IsNullOrEmpty(current))
        {
            if (File.Exists(Path.Combine(current, ".env")))
            {
                return current;
            }

            current = Directory.GetParent(current)?.FullName;
        }

        return Directory.GetCurrentDirectory();
    }

    private static string GetValue(string key, string fallback)
    {
        string? value = ReadEnvFile().FirstOrDefault(item => item.Key.Equals(key, StringComparison.OrdinalIgnoreCase)).Value;
        return string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();
    }

    private static int GetIntValue(string key, int fallback)
    {
        string value = GetValue(key, fallback.ToString(CultureInfo.InvariantCulture));
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int result) && result is >= 1 and <= 65535
            ? result
            : fallback;
    }

    private static Dictionary<string, string> ReadEnvFile()
    {
        string path = Path.Combine(FindProjectRoot(), ".env");
        if (!File.Exists(path))
        {
            return new Dictionary<string, string>();
        }

        return File.ReadAllLines(path)
            .Select(line => line.Trim())
            .Where(line => !string.IsNullOrWhiteSpace(line) && !line.StartsWith('#'))
            .Select(line => line.Split('=', 2))
            .Where(parts => parts.Length == 2)
            .ToDictionary(parts => parts[0].Trim(), parts => parts[1].Trim().Trim('"'), StringComparer.OrdinalIgnoreCase);
    }
}
