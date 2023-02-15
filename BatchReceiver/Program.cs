using Azure.Identity;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Storage.Blobs;
using BatchReceiver;
using Microsoft.Extensions.Configuration;

var configuration = GetConfiguration();

var eventHubConfig = new EventHubConfig();
configuration.Bind(EventHubConfig.ConfigSection, eventHubConfig);

var storageClient = new BlobContainerClient(
    new Uri(eventHubConfig.BlobStorage),
    new DefaultAzureCredential());

var consumerGroup = eventHubConfig.ConsumerGroupName != null
    ? eventHubConfig.ConsumerGroupName
    : EventHubConsumerClient.DefaultConsumerGroupName;

var maximumBatchSize = 100;

var processor = new ApplicationEventHubBatchProcessor(
    storageClient,
    maximumBatchSize,
    consumerGroup,
    eventHubConfig.NamespaceName,
    eventHubConfig.Name,
    new DefaultAzureCredential());

using var cancellationSource = new CancellationTokenSource();

Console.WriteLine("Processing started");
Console.WriteLine($"Consumer group {consumerGroup}");

await processor.StartProcessingAsync();

Console.ReadLine();

await processor.StopProcessingAsync();

await Task.Delay(TimeSpan.FromSeconds(7));


static IConfiguration GetConfiguration()
{
    return new ConfigurationManager()
    .AddJsonFile("appsettings.json")
    .Build();
}