using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy;
using Extension.Confluent.Kafka.Client.Extensions;
using Extension.Confluent.Kafka.Client.Health;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Consumer
{
    internal class ConsumeResultDispatcher<TKey, TValue> : IDisposable
    {
        private const int BackOffMilliseconds = 50;
        private const byte DefaultPriority = 0;

        private readonly IConsumeResultCallback<TKey, TValue> callback;
        private readonly ConsumeResultChannelWorkerConfig configuration;
        private readonly IDispatcherStrategy<TKey, TValue> dispatcherStrategy;
        private readonly IHealthStatusCallback? healthStatusCallback;
        private readonly ConcurrentDictionary<long, Task> workers;
        private readonly ILogger logger;

        public ConsumeResultDispatcher(IConsumeResultCallback<TKey, TValue> callback,
            IDispatcherStrategy<TKey, TValue> dispatcherStrategy,
            IHealthStatusCallback? healthStatusCallback,
            ConsumeResultChannelWorkerConfig configuration,
            ILogger logger)
        {
            this.callback = callback ?? throw new ArgumentNullException(nameof(callback));
            this.configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            this.dispatcherStrategy = dispatcherStrategy ?? throw new ArgumentNullException(nameof(dispatcherStrategy));
            this.workers = new ConcurrentDictionary<long, Task>();
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.healthStatusCallback = healthStatusCallback;
        }

        public async Task<bool> TryEnqueueAsync(ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken)
        {
            do
            {
                var priority = result.Message?.Headers?.GetTopicPriority() ?? DefaultPriority;

                if (dispatcherStrategy.CreateOrGet(result, out var channel))
                {
                    result.Message?.Headers?.AddWorkerChannelId(channel.Id);

                    //Note: work should only be canceled by dispose
                    var workerTask = CreateWorkerTask(channel, cancellationToken);
                    if (!workers.TryAdd(channel.Id, workerTask))
                    {
                        // in order to be thread-safe, we retry
                        continue;
                    }

                    //Note: start worker task before write into channel
                    workerTask.Start();

                    if (!channel.TryWrite(result, priority))
                    {
                        return false;
                    }

                    return true;
                }

                //Note: header doesn't have to be unique, in order to simplify logic we add header again in case of retry
                result.Message?.Headers?.AddWorkerChannelId(channel.Id);

                //Note: we allows one retry in order to prevent impact on short hickup 
                if (!channel.TryWrite(result, priority))
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(BackOffMilliseconds), cancellationToken).ConfigureAwait(false);

                    return channel.TryWrite(result, priority);
                }

                return true;

            } while (!cancellationToken.IsCancellationRequested);

            return false;
        }

        private Task CreateWorkerTask(IConsumeResultChannel<TKey, TValue> channel, CancellationToken cancellationToken)
        {
            var worker = new ConsumeResultChannelWorker<TKey, TValue>(channel, callback, healthStatusCallback, CleanupChannel, configuration, logger);
            return worker.CreateRunTask(cancellationToken);
        }

        public void CleanupChannel(IConsumeResultChannel<TKey, TValue> channel)
        {
            try
            {
                workers.TryRemove(channel.Id, out _);
                dispatcherStrategy.Remove(channel);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error during dispatcher channel cleanup:");
            }
        }


        public void Dispose()
        {
            dispatcherStrategy.Dispose();
        }
    }
}
