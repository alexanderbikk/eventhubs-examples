using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Receiver1;
using Shared;

var configuration = GetConfiguration();

var eventHubConfig = new EventHubConfig();
configuration.Bind(EventHubConfig.ConfigSection, eventHubConfig);

var storageClient = new BlobContainerClient(
    new Uri(eventHubConfig.BlobStorage),
    new DefaultAzureCredential());

var consumerGroup = eventHubConfig.ConsumerGroupName != null
    ? eventHubConfig.ConsumerGroupName
    : EventHubConsumerClient.DefaultConsumerGroupName;

var processor = new EventProcessorClient(
    storageClient,
    consumerGroup,
    eventHubConfig.NamespaceName,
    eventHubConfig.Name,
    new DefaultAzureCredential());

// Register handlers for processing events and handling errors
processor.ProcessEventAsync += ApplicationEventHubProcessor.ProcessEventHandler;
processor.ProcessErrorAsync += ApplicationEventHubProcessor.ProcessErrorHandler;

processor.PartitionInitializingAsync += ApplicationEventHubProcessor.PartitionInitHandler;
processor.PartitionClosingAsync += ApplicationEventHubProcessor.PartitionClosingHandler;

Console.WriteLine("Processing started");
Console.WriteLine($"Consumer group {consumerGroup}");

// Start the processing
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