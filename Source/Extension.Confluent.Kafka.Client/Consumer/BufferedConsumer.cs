using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Configuration;
using Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy;
using Extension.Confluent.Kafka.Client.Health;
using Extension.Confluent.Kafka.Client.Metrics;
using Extension.Confluent.Kafka.Client.OffsetStore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Consumer
{
    internal class BufferedConsumer<TKey, TValue> : IBufferedConsumer<TKey, TValue>
    {
        private const double ConsumeTimeoutSeconds = 5;

        private readonly object lockObject = new object();
        private readonly HashSet<string> subscribedTopics;

        private readonly ILogger logger;

        private readonly IAdminClient adminClient;
        private readonly IConsumer<TKey, TValue> internalConsumer;
        private readonly IHealthStatusCallback? healthStatusCallback;
        private readonly IOffsetStore<TKey, TValue> offsetStore;

        private readonly ConsumeResultDispatcher<TKey, TValue> dispatcher;
        private readonly ConsumeResultStream<TKey, TValue> stream;

        private readonly BufferedConsumerConfig configuration;
        //Note: token source is used to control message loop and worker tasks is controlled by Assign or Unsubscribe
        private CancellationTokenSource? cts;
        private bool connectionHealthy = false;

        public BufferedConsumer(IConsumerBuilderWrapper<TKey, TValue> internalConsumerBuilder,
            IAdminClient adminClient,
            Func<IConsumer<TKey, TValue>, IOffsetStore<TKey, TValue>> createOffsetStoreFunc,
            IDispatcherStrategy<TKey, TValue> dispatcherStrategy,
            IConsumeResultCallback<TKey, TValue>? callback,
            IHealthStatusCallback? healthStatusCallback,
            IMetricsCallback? metricsCallback,
            BufferedConsumerConfig configuration,
            ILogger logger)
        {
            if (internalConsumerBuilder == null) throw new ArgumentNullException(nameof(internalConsumerBuilder));
            if (createOffsetStoreFunc == null) throw new ArgumentNullException(nameof(createOffsetStoreFunc));

            //Note: create interface 
            this.internalConsumer = internalConsumerBuilder
                    .SetPartitionsAssignedHandler((_, list) => AssignPartitionsCallback(list))
                    .SetPartitionsRevokedHandler((_, list) => RevokePartitionsCallback(list))
                .Build();

            this.adminClient = adminClient ?? throw new ArgumentNullException(nameof(adminClient));
            this.offsetStore = createOffsetStoreFunc(this.internalConsumer) ?? throw new ArgumentException($"invalid {nameof(createOffsetStoreFunc)} func");
            this.configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            this.healthStatusCallback = healthStatusCallback ?? throw new ArgumentNullException(nameof(healthStatusCallback));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.dispatcher = new ConsumeResultDispatcher<TKey, TValue>(
                new ConsumeResultCallbackWrapper(callback, metricsCallback, offsetStore, logger),
                dispatcherStrategy,
                new ConsumeResultChannelWorkerConfig { CallbackResultCount = configuration.CallbackResultCount },
                logger);
            this.stream = new ConsumeResultStream<TKey, TValue>(internalConsumer, configuration.TopicConfigs, logger);
            this.subscribedTopics = new HashSet<string>(configuration.TopicConfigs.Select(_ => _.TopicName));
        }

        private Task StartMessageLoopTask()
        {
            logger.LogInformation("Starting kafka message consumption loop");

            if (cts == null) throw new InvalidOperationException("CancellationTokenSource is missing");

            return Task.Run(async () =>
            {
                var cancellationToken = cts.Token;
                var flushCommit = DateTime.MinValue;
                var lastLoopHeartbeat = DateTime.MinValue;
                //Note: DateTime-Type is used to define some cooldown period in case of partitions are paused
                var paused = DateTime.MaxValue;
                var pausedOffsets = new List<TopicPartitionOffset>();

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        if (!connectionHealthy)
                        {
                            CheckBrokerConnection();
                        }

                        if (paused < DateTime.UtcNow && pausedOffsets.Count > 0)
                        {
                            //note: Pause and Resume are managed within single task, there should be no race-conditions
                            stream.Resume(pausedOffsets);
                            pausedOffsets.Clear();
                            paused = DateTime.MaxValue;

                            logger.LogInformation("Resume consumption from all kafka topic partitions");
                        }

                        foreach (var result in stream.Consume(TimeSpan.FromSeconds(ConsumeTimeoutSeconds)))
                        {
                            if (!subscribedTopics.Contains(result.Topic))
                                return;

                            try
                            {
                                var queued = await dispatcher.TryEnqueueAsync(result, cancellationToken);
                                if (!queued)
                                {
                                    paused = DateTime.UtcNow + TimeSpan.FromMilliseconds(configuration.BackpressurePartitionPauseInMilliseconds);
                                    var pausedOffset = stream.Pause(result);
                                    if (pausedOffset != null)
                                    {
                                        pausedOffsets.Add(pausedOffset);
                                    }

                                    logger.LogWarning($"Enqueue message for processing failed due to processing timeout, partition {result.TopicPartition} is paused.");
                                }
                            }
                            catch (Exception ex)
                            {
                                logger.LogError(ex, $"Enqueue message for processing failed: {result.TopicPartitionOffset}.");
                            }
                        }

                        if (cancellationToken.IsCancellationRequested)
                        {
                            break;
                        }

                        if (flushCommit < DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(configuration.BufferCommitIntervalInMilliseconds)))
                        {
                            //Note: we commit offset based on the internal offset store then the sharding strategy defines the offset commit strategy
                            offsetStore?.FlushCommit();
                            flushCommit = DateTime.UtcNow;
                        }

                        //Note: The heartbeat should be only set according to interval
                        if (configuration.PingIntervalInMilliseconds.HasValue &&
                           lastLoopHeartbeat < DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(configuration.PingIntervalInMilliseconds.Value)))
                        {
                            healthStatusCallback?.OnMessageLoopPing();
                            lastLoopHeartbeat = DateTime.UtcNow;
                        }
                    }
                    catch (TaskCanceledException tce) when (tce.CancellationToken == cancellationToken)
                    {
                        break;
                    }
                    catch (ConsumeException ex)
                    {
                        connectionHealthy = false;
                        healthStatusCallback?.OnUnhealthyConnection(ex);
                        logger.LogError(ex, "Kafka message consumption failed");
                    }
                    catch (Exception ex)
                    {
                        connectionHealthy = false;
                        healthStatusCallback?.OnUnhealthyConnection(ex);
                        logger.LogError(ex, "Kafka message consumption restarted");
                    }
                }

                var reason = (cancellationToken.IsCancellationRequested ? ": cancellation was requested" : string.Empty);
                logger.LogInformation($"Finished kafka message consumption loop: {reason}");
            });
        }

        private void CheckBrokerConnection()
        {
            if (adminClient == null) throw new InvalidOperationException($"{nameof(adminClient)} not initialized");

            try
            {
                //Note: in order to avoid ACL warning flooding on kafka broker side we take topic where user has described rights
                var subscribedTopic = subscribedTopics.FirstOrDefault();
                if (subscribedTopic != null)
                {
                    adminClient.GetMetadata(subscribedTopic, TimeSpan.FromMilliseconds(configuration.ConnectionTimeoutInMilliseconds));
                    connectionHealthy = true;
                    healthStatusCallback?.OnHealthyConnection();
                }
            }
            catch (Exception ex)
            {
                connectionHealthy = false;
                healthStatusCallback?.OnUnhealthyConnection(ex);
            }
        }

        public void Subscribe()
        {
            lock (lockObject)
            {
                logger.LogInformation($"Subcribe for topics {string.Join(",", subscribedTopics)} via buffering");

                stream.Subscribe();
            }
        }

        public void Unsubcribe()
        {
            lock (lockObject)
            {
                logger.LogInformation($"Starting to unsubscribe and stop processing for topics  {string.Join(",", subscribedTopics)}");

                stream.Unsubscribe();

                cts?.Cancel();
                cts?.Dispose();
                cts = null;

                //Note: to prevent reprocessing since offsets are committed in intervals
                offsetStore.FlushCommit();
                offsetStore.Clear();

                logger.LogInformation($"Successfully unsubscribed & stopped processing for topics {string.Join(", ", subscribedTopics)}");
            }
        }

        public void Dispose()
        {
            Unsubcribe();
            this.internalConsumer.Dispose();
            this.offsetStore.Dispose();
        }

        private void AssignPartitionsCallback(IEnumerable<TopicPartition> assignedPartitions)
        {
            if (stream == null) throw new InvalidOperationException($"{nameof(stream)} not initialized");

            lock (lockObject)
            {
                var assignedTopicsMessage = string.Join(",", assignedPartitions.Select(_ => _.ToString()));

                logger.LogInformation($"assign partitions [{assignedTopicsMessage}]");
                stream.Assign(assignedPartitions);

                //note: in order cleanup existing worker channel tasks we cancel existing processing
                cts?.Cancel();
                cts?.Dispose();
                cts = new CancellationTokenSource();

                logger.LogInformation("start message loop started by new paritions {assignedTopicsMessage} ");

                _ = StartMessageLoopTask();
            }
        }

        private void RevokePartitionsCallback(IEnumerable<TopicPartitionOffset> revokedPartitions)
        {
            if (stream == null) throw new InvalidOperationException($"{nameof(stream)} not initialized");

            lock (lockObject)
            {
                try
                {
                    //Note: to prevent reprocessing since offsets are committed in intervals
                    offsetStore.FlushCommit();
                    offsetStore.Clear(revokedPartitions);
                }
                finally
                {
                    stream.UnAssign(revokedPartitions);
                }
            }
        }

        private class ConsumeResultCallbackWrapper : IConsumeResultCallback<TKey, TValue>
        {
            private readonly IOffsetStore<TKey, TValue> offsetStore;
            private readonly IConsumeResultCallback<TKey, TValue>? callback;
            private readonly IMetricsCallback? metricsCallback;
            private readonly ILogger logger;

            public ConsumeResultCallbackWrapper(IConsumeResultCallback<TKey, TValue>? callback,
                IMetricsCallback? metricsCallback,
                IOffsetStore<TKey, TValue> offsetStore,
                ILogger logger)
            {
                this.callback = callback;
                this.metricsCallback = metricsCallback;
                this.offsetStore = offsetStore ?? throw new ArgumentNullException(nameof(offsetStore));
                this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            }

            public async Task OnReceivedAsync(ReadOnlyMemory<ConsumeResult<TKey, TValue>> results, CancellationToken cancellationToken)
            {
                //Note: before processing is started the message offset is stored for commit
                offsetStore.Store(results);

                var stopwatch = Stopwatch.StartNew();

                try
                {
                    if (callback != null)
                    {
                        await callback.OnReceivedAsync(results, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, $"Message processing failed. But message will committed.");
                }
                finally
                {
                    metricsCallback?.OnReceived(results.Length, stopwatch.Elapsed);
                }

                //Note: processed message is marked as complete so the offset can be flushed and committed
                offsetStore.Complete(results);
            }
        }
    }
}
