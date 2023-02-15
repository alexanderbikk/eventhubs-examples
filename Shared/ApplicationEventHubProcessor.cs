using System.Text;
using Azure.Messaging.EventHubs.Processor;

namespace Shared
{
    public class ApplicationEventHubProcessor
    {
        public static Task PartitionInitHandler(PartitionInitializingEventArgs eventArgs)
        {
            Console.WriteLine($"Partition {eventArgs.PartitionId} initialized");
            return Task.CompletedTask;
        }

        public static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tReceived event: {0}, Partition {1}",
                Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()), eventArgs.Partition.PartitionId);

            //enable to show no checkpointing
            //if (eventArgs.Partition.PartitionId == "0")
            //{
            //    return;
            //}

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        public static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': " +
                $"an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }

        public static Task PartitionClosingHandler(PartitionClosingEventArgs eventArgs)
        {
            Console.WriteLine($"Partition {eventArgs.PartitionId} closing");
            return Task.CompletedTask;
        }
    }
}