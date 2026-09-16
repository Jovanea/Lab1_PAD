using System.Globalization;

namespace MessageHub.Configuration;

public static class EnvironmentSettings
{
    public static string BrokerHost => GetValue("BROKER_HOST", "127.0.0.1");

    public static int BrokerPort => GetIntValue("BROKER_PORT", 5000);

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
