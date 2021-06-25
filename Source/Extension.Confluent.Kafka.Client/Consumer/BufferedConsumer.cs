using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Builder;
using Extension.Confluent.Kafka.Client.Configuration;
using Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy;
using Extension.Confluent.Kafka.Client.Health;
using Extension.Confluent.Kafka.Client.Metrics;
using Extension.Confluent.Kafka.Client.OffsetStore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
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

        //Note: token source is used to control message loop
        private CancellationTokenSource messageLoopCancellationTokenSource;

        //Note: token sources to control worker cancellation in case of rebalance
        private ConcurrentDictionary<TopicPartition, CancellationTokenSource> workerTaskCancellationTokenSourceByTopicPartition;

        private bool connectionHealthy = false;

        public BufferedConsumer(IConsumerBuilderWrapper<TKey, TValue> consumerBuilder,
            IAdminClientBuilderWrapper adminClientBuilder,
            Func<IConsumer<TKey, TValue>, IOffsetStore<TKey, TValue>> createOffsetStoreFunc,
            IDispatcherStrategy<TKey, TValue> dispatcherStrategy,
            IConsumeResultCallback<TKey, TValue>? callback,
            IHealthStatusCallback? healthStatusCallback,
            IMetricsCallback? metricsCallback,
            BufferedConsumerConfig configuration,
            ILogger logger)
        {
            if (consumerBuilder == null) throw new ArgumentNullException(nameof(consumerBuilder));
            if (adminClientBuilder == null) throw new ArgumentNullException(nameof(adminClientBuilder));
            if (createOffsetStoreFunc == null) throw new ArgumentNullException(nameof(createOffsetStoreFunc));

            this.internalConsumer = consumerBuilder
                    .SetPartitionsAssignedHandler((_, list) => RegisterWrokerCtsOnAssign(list))
                    .SetPartitionsRevokedHandler((_, list) => CommittOffsetsOnRevoke(list))
                .Build() ?? throw new ArgumentNullException(nameof(consumerBuilder));

            this.adminClient = adminClientBuilder.Build() ?? throw new ArgumentNullException(nameof(adminClient));
            this.offsetStore = createOffsetStoreFunc?.Invoke(this.internalConsumer) ?? throw new ArgumentException($"invalid {nameof(createOffsetStoreFunc)} func");
            this.configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            this.healthStatusCallback = healthStatusCallback;
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.dispatcher = new ConsumeResultDispatcher<TKey, TValue>(
                new ConsumeResultCallbackWrapper(callback, metricsCallback, offsetStore, logger),
                dispatcherStrategy,
                healthStatusCallback,
                new ConsumeResultChannelWorkerConfig { CallbackResultCount = configuration.CallbackResultCount },
                logger);
            this.stream = new ConsumeResultStream<TKey, TValue>(internalConsumer, configuration.TopicConfigs, logger);
            this.subscribedTopics = new HashSet<string>(configuration.TopicConfigs.Select(_ => _.TopicName));
            this.workerTaskCancellationTokenSourceByTopicPartition = new ConcurrentDictionary<TopicPartition, CancellationTokenSource>();
            this.messageLoopCancellationTokenSource = new CancellationTokenSource();
        }

        private Task StartMessageLoopTask(CancellationTokenSource cancellationTokenSource)
        {
            logger.LogInformation("Starting kafka message consumption loop");

            return Task.Run(async () =>
            {
                var cancellationToken = cancellationTokenSource.Token;
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
                            try
                            {
                                bool queued = false;
                                if (workerTaskCancellationTokenSourceByTopicPartition.TryGetValue(result.TopicPartition, out var workerCts))
                                {
                                    queued = await dispatcher.TryEnqueueAsync(result, workerCts.Token).ConfigureAwait(false);
                                }
                                else
                                {
                                    logger.LogWarning($"Worker Cancellation Token for {result.TopicPartition} is missing.");
                                }

                                if (!queued)
                                {
                                    paused = DateTime.UtcNow + TimeSpan.FromMilliseconds(configuration.BackpressurePartitionPauseInMilliseconds);
                                    var pausedOffset = stream.Pause(result);
                                    if (pausedOffset != null)
                                    {
                                        pausedOffsets.Add(pausedOffset);
                                    }

                                    logger.LogWarning($"Enqueue message for processing failed due to processing timeout or missing cts, partition {result.TopicPartition} is paused.");
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
                    catch (OperationCanceledException tce) when (tce.CancellationToken == cancellationToken)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        connectionHealthy = false;
                        healthStatusCallback?.OnUnhealthyConnection(ex);
                        logger.LogError(ex, "Kafka message loop restarted");
                    }
                }

                var reason = (cancellationToken.IsCancellationRequested ? ": cancellation was requested" : string.Empty);
                healthStatusCallback?.OnMessageLoopCancelled(reason);
                logger.LogInformation($"Finished kafka message consumption loop: {reason}");
            });
        }

        private void CheckBrokerConnection()
        {
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

                messageLoopCancellationTokenSource.Cancel();
                messageLoopCancellationTokenSource.Dispose();
                messageLoopCancellationTokenSource = new CancellationTokenSource();

                _ = StartMessageLoopTask(messageLoopCancellationTokenSource);
            }
        }

        public void Unsubscribe()
        {
            lock (lockObject)
            {
                logger.LogInformation($"Starting to unsubscribe and stop processing for topics  {string.Join(",", subscribedTopics)}");

                stream.Unsubscribe();

                //Note: only cancelled, cts will be disposed and set again on subscribe
                messageLoopCancellationTokenSource.Cancel();

                //Note: to prevent reprocessing since offsets are committed in intervals
                offsetStore.FlushCommit();
                offsetStore.Clear();

                foreach (var topicPartition in workerTaskCancellationTokenSourceByTopicPartition.Keys.ToArray())
                {
                    if (workerTaskCancellationTokenSourceByTopicPartition.TryRemove(topicPartition, out var cts))
                    {
                        cts.Cancel();
                        cts.Dispose();
                    }
                }

                logger.LogInformation($"Successfully unsubscribed & stopped processing for topics {string.Join(", ", subscribedTopics)}");
            }
        }

        public void Dispose()
        {
            Unsubscribe();
            this.messageLoopCancellationTokenSource.Dispose();
            this.internalConsumer.Dispose();
            this.offsetStore.Dispose();
            this.dispatcher.Dispose();
        }

        private void RegisterWrokerCtsOnAssign(IEnumerable<TopicPartition> assignedPartitions)
        {
            lock (lockObject)
            {
                var topicPartitions = new HashSet<TopicPartition>(assignedPartitions);

                var assignedTopicsMessage = string.Join(",", topicPartitions.Select(_ => _.ToString()));

                logger.LogInformation($"assign partitions [{assignedTopicsMessage}]");

                //Note: add not existing cts for worker
                foreach (var topicPartition in topicPartitions)
                {
                    if (!workerTaskCancellationTokenSourceByTopicPartition.ContainsKey(topicPartition))
                    {
                        workerTaskCancellationTokenSourceByTopicPartition[topicPartition] = CancellationTokenSource.CreateLinkedTokenSource(messageLoopCancellationTokenSource.Token);
                    }
                }

                //Note: cancel cts for not used topic partitions
                foreach (var topicPartition in workerTaskCancellationTokenSourceByTopicPartition.Keys.ToArray())
                {
                    if (!topicPartitions.Contains(topicPartition))
                    {
                        if (workerTaskCancellationTokenSourceByTopicPartition.TryRemove(topicPartition, out var cts))
                        {
                            cts.Cancel();
                            cts.Dispose();
                        }
                    }
                }

                stream.Assign(topicPartitions);
            }
        }

        private void CommittOffsetsOnRevoke(IEnumerable<TopicPartitionOffset> revokedPartitions)
        {
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

                    metricsCallback?.RecordFailure(results.Length, stopwatch.Elapsed);
                }
                finally
                {
                    metricsCallback?.RecordSuccess(results.Length, stopwatch.Elapsed);
                }

                //Note: processed message is marked as complete so the offset can be flushed and committed
                offsetStore.Complete(results);
            }
        }
    }
}
