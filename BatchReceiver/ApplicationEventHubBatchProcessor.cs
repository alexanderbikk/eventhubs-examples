using Azure.Core;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Primitives;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

namespace BatchReceiver;

public class ApplicationEventHubBatchProcessor : PluggableCheckpointStoreEventProcessor<EventProcessorPartition>
{
    // This example uses a connection string, so only the single constructor
    // was implemented; applications will need to shadow each constructor of
    // the PluggableCheckpointStoreEventProcessor that they are using.

    public ApplicationEventHubBatchProcessor(
        BlobContainerClient storageClient,
        int eventBatchMaximumCount,
        string consumerGroup,
        string eventHubNamespace,
        string eventHubName,
        TokenCredential tokenCredential,
        EventProcessorOptions? clientOptions = default)
            : base(
                new BlobCheckpointStore(storageClient),
                eventBatchMaximumCount,
                consumerGroup,
                eventHubNamespace,
                eventHubName,
                tokenCredential,
                clientOptions)
    {
    }

    protected override async Task OnProcessingEventBatchAsync(
        IEnumerable<EventData> events,
        EventProcessorPartition partition,
        CancellationToken cancellationToken)
    {
        EventData? lastEvent = null;

        try
        {
            Console.WriteLine($"Received events for partition {partition.PartitionId}, " +
                $"events count {events.Count()}");

            foreach (var currentEvent in events)
            {
                Console.WriteLine($"Event: {currentEvent.EventBody}");
                lastEvent = currentEvent;
            }

            if (lastEvent != null)
            {
                await UpdateCheckpointAsync(
                    partition.PartitionId,
                    lastEvent.Offset,
                    lastEvent.SequenceNumber,
                    cancellationToken)
                .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            // It is very important that you always guard against exceptions in
            // your handler code; the processor does not have enough
            // understanding of your code to determine the correct action to take.
            // Any exceptions from your handlers go uncaught by the processor and
            // will NOT be redirected to the error handler.
            //
            // In this case, the partition processing task will fault and be restarted
            // from the last recorded checkpoint.

            Console.WriteLine($"Exception while processing events: {ex}");
        }
    }

    protected override Task OnProcessingErrorAsync(
        Exception exception,
        EventProcessorPartition partition,
        string operationDescription,
        CancellationToken cancellationToken)
    {
        try
        {
            if (partition != null)
            {
                Console.Error.WriteLine(
                    $"Exception on partition {partition.PartitionId} while " +
                    $"performing {operationDescription}: {exception}");
            }
            else
            {
                Console.Error.WriteLine(
                    $"Exception while performing {operationDescription}: {exception}");
            }
        }
        catch (Exception ex)
        {
            // It is very important that you always guard against exceptions
            // in your handler code; the processor does not have enough
            // understanding of your code to determine the correct action to
            // take.  Any exceptions from your handlers go uncaught by the
            // processor and will NOT be handled in any way.
            //
            // In this case, unhandled exceptions will not impact the processor
            // operation but will go unobserved, hiding potential application problems.

            Console.WriteLine($"Exception while processing events: {ex}");
        }

        return Task.CompletedTask;
    }

    protected override Task OnInitializingPartitionAsync(
        EventProcessorPartition partition,
        CancellationToken cancellationToken)
    {
        try
        {
            Console.WriteLine($"Partition {partition.PartitionId} initialized");
        }
        catch (Exception ex)
        {
            // It is very important that you always guard against exceptions in
            // your handler code; the processor does not have enough
            // understanding of your code to determine the correct action to take.
            // Any exceptions from your handlers go uncaught by the processor and
            // will NOT be redirected to the error handler.
            //
            // In this case, the partition processing task will fault and the
            // partition will be initialized again.

            Console.WriteLine($"Exception while initializing a partition: {ex}");
        }

        return Task.CompletedTask;
    }

    protected override Task OnPartitionProcessingStoppedAsync(
        EventProcessorPartition partition,
        ProcessingStoppedReason reason,
        CancellationToken cancellationToken)
    {
        try
        {
            Console.WriteLine(
                $"Partition {partition.PartitionId} closing " +
                $"because {reason}");
        }
        catch (Exception ex)
        {
            // It is very important that you always guard against exceptions in
            // your handler code; the processor does not have enough
            // understanding of your code to determine the correct action to take.
            // Any exceptions from your handlers go uncaught by the processor and
            // will NOT be redirected to the error handler.
            //
            // In this case, unhandled exceptions will not impact the processor
            // operation but will go unobserved, hiding potential application problems.

            Console.WriteLine($"Exception while stopping processing for a partition: {ex}");
        }

        return Task.CompletedTask;
    }
}
