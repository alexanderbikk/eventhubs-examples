namespace BatchReceiver;

public class EventHubConfig
{
    public static string ConfigSection = "EventHub";

    public string? NamespaceName { get; init; }

    public string? Name { get; init; }

    public string? ConsumerGroupName { get; init; }

    public string? BlobStorage { get; init;}
}
