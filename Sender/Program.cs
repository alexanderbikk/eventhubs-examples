using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Configuration;
using Sender;
using Azure.Identity;

var configuration = GetConfiguration();

var eventHubConfig = new EventHubConfig();
configuration.Bind(EventHubConfig.ConfigSection, eventHubConfig);


var producerClient = new EventHubProducerClient(
    eventHubConfig.NamespaceName,
    eventHubConfig.Name,
    new DefaultAzureCredential());

var numOfEvents = 3;
try
{
    await SendEventsAsync(producerClient, numOfEvents);
}
finally
{
    await producerClient.DisposeAsync();
}

static async Task SendEventsAsync(EventHubProducerClient producerClient, int numOfEvents)
{
    for (var j = 0; j < numOfEvents * 2; j++)
    {
        // Create a batch of events 
        using var eventBatch = await producerClient.CreateBatchAsync();

        for (var i = 1; i <= numOfEvents; i++)
        {
            if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {j}_{i}"))))
            {
                // if it is too large for the batch
                throw new Exception($"Event {j}_{i} is too large for the batch and cannot be sent.");
            }
        }

        // Use the producer client to send the batch of events to the event hub
        await producerClient.SendAsync(eventBatch);
        Console.WriteLine($"A batch of {numOfEvents} events has been published.");
    }
}

static IConfiguration GetConfiguration()
{
    return new ConfigurationManager()
    .AddJsonFile("appsettings.json")
    .Build();
}