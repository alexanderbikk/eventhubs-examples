namespace Sender;

public class EventHubConfig
{
    public static string ConfigSection = "EventHub";

    public string? NamespaceName { get; init; }

    public string? Name { get; init; }
}
