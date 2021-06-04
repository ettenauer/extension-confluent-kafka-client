using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Consumer
{
    internal class ConsumeResultChannelWorker<TKey, TValue>
    {
        private readonly IConsumeResultChannel<TKey, TValue> channel;
        private readonly ConsumeResultChannelWorkerConfig configuration;
        private readonly IConsumeResultCallback<TKey, TValue> callback;
        private readonly ILogger logger;
        

        public ConsumeResultChannelWorker(IConsumeResultChannel<TKey, TValue> channel, 
            IConsumeResultCallback<TKey, TValue> callback,
            ConsumeResultChannelWorkerConfig configuration,
            ILogger logger)
        {
            this.channel = channel ?? throw new ArgumentNullException(nameof(channel));
            this.callback = callback ?? throw new ArgumentNullException(nameof(callback));
            this.configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public Task CreateRunTask(CancellationToken cancellationToken)
        {
            return new Task(async () =>
            {
                var memoryOwner = MemoryPool<ConsumeResult<TKey, TValue>>.Shared.Rent(configuration.CallbackResultCount);
                try
                {
                    while (!cancellationToken.IsCancellationRequested && await channel.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        //IMPORTANT:
                        // there is a memory leak in case low priority topic never receive a message due to not cleanup AsyncOperation reference in BoundedChannel
                        // https://github.com/dotnet/runtime/issues/761
                        // workaround for .net 3.1 would to write in both channel a fake item to force cleanup of cancelled operations since we try to consume all available messages after unblocking
                        channel.WriteFake();

                        var bufferedResults = memoryOwner.Memory;
                        var index = 0;
                        while (index < configuration.CallbackResultCount && channel.TryRead(out var result))
                        {
                            //IMPORTANT: skip fake message
                            if (result != null)
                            {
                                bufferedResults.Span[index] = result;
                                index++;
                            }
                        }

                        if (index > 0)
                        {
                            await callback.OnReceivedAsync(bufferedResults.Slice(0, index), cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
                catch (TaskCanceledException tce) when (tce.CancellationToken == cancellationToken)
                {
                    logger.LogInformation($"work {channel.Id} terminated due to cancellation");
                }
                catch (Exception e)
                {
                    logger.LogError(e, $"work failed for queue {channel.Id} due to:");
                }
                finally
                {
                    memoryOwner.Dispose();
                }

                logger.LogInformation($"work {channel.Id} terminated");
            });
        }
    }
}
