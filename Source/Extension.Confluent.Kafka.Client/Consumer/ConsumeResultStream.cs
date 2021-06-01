using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Configuration;
using Extension.Confluent.Kafka.Client.Extensions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Extension.Confluent.Kafka.Client.Consumer
{
    internal class ConsumeResultStream<TKey, TValue>
    {
        private readonly ILogger logger;
        private readonly IConsumer<TKey, TValue> consumer;

        private List<TopicPartition>? assignedTopicPartitions;

        private readonly Dictionary<string, byte> priorityByTopic;

        public ConsumeResultStream(IConsumer<TKey, TValue> consumer, IEnumerable<BufferedTopicConfig> configuration, ILogger logger)
        {
            this.consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            if(configuration == null)throw new ArgumentNullException(nameof(configuration));
            this.priorityByTopic = configuration.ToDictionary(_ => _.TopicName, _ => _.Priority);
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public void Subscribe()
        {
            consumer.Subscribe(priorityByTopic.Keys);
        }

        public void Unsubscribe()
        {
            consumer.Unsubscribe();
        }

        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            assignedTopicPartitions = partitions.ToList();
        }

        public void UnAssign(IEnumerable<TopicPartitionOffset> topicPartitions)
        {
            assignedTopicPartitions = assignedTopicPartitions?.Except(topicPartitions.Select(_ => _.TopicPartition)).ToList();
        }

        public IEnumerable<ConsumeResult<TKey, TValue>> Consume(TimeSpan timeout)
        {
            var newResult = consumer.Consume(timeout);
            if (newResult != null)
            {
                //Note: add here priority to Header for downstream processing
                if (newResult.Message.Headers != null && priorityByTopic.ContainsKey(newResult.Topic))
                {
                    _ = newResult.Message.Headers.AddTopicPriority(priorityByTopic[newResult.Topic]);
                }

                yield return newResult;
            }
        }

        public TopicPartitionOffset? Pause(ConsumeResult<TKey, TValue> result)
        {
            var topicPartition = assignedTopicPartitions?.Where(k => k.Topic == result.Topic && k.Partition == result.Partition);
            if (topicPartition != null && topicPartition.Any())
            {
                consumer.Pause(topicPartition);
                return result.TopicPartitionOffset;
            }

            logger.LogWarning($"Pause discarded since no assigned TopicPartition for {result.TopicPartition}");
            return null;
        }

        public void Resume(List<TopicPartitionOffset> pausedOffsets)
        {
            if (pausedOffsets.Count > 0 && (assignedTopicPartitions?.Count ?? 0) > 0)
            {
                var resumeTopicPartitions = new List<TopicPartition>();
                foreach (var pausedTopicPartitions in pausedOffsets.GroupBy(_ => _.TopicPartition.ToString(), _ => _))
                {
                    if (assignedTopicPartitions!.Exists(_ => _.ToString() == pausedTopicPartitions.Key))
                    {
                        var earliestPause = pausedTopicPartitions.OrderBy(_ => _.Offset.Value).First();
                        consumer.Seek(earliestPause);
                        resumeTopicPartitions.Add(earliestPause.TopicPartition);
                    }
                    else
                    {
                        logger.LogInformation("Cannot resume {0} because not defined as assigned TopicPartition", pausedTopicPartitions.Key);
                    }
                }

                if (resumeTopicPartitions.Count > 0)
                {
                    consumer.Resume(resumeTopicPartitions);
                }
            }
            else
            {
                logger.LogWarning("Resume discarded since no assigned TopicPartitions");
            }
        }
    }
}
