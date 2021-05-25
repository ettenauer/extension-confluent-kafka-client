using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Configuration;
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

        private readonly IEnumerable<BufferedTopicConfig> configuration;

        public ConsumeResultStream(IConsumer<TKey, TValue> consumer, IEnumerable<BufferedTopicConfig> configuration, ILogger logger)
        {
            this.consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            this.configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            this.logger = logger ?? throw new ArgumentException(nameof(logger));
        }

        public void Subscribe()
        {
            consumer.Subscribe(configuration.Select(s => s.TopicName));
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

        public IList<TopicPartition> GetAssignment()
        {
            return consumer.Assignment;
        }

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
        {
            return consumer.GetWatermarkOffsets(topicPartition);
        }

        public IEnumerable<ConsumeResult<TKey, TValue>> Consume(TimeSpan timeout)
        {
            var newResult = consumer.Consume(timeout);
            if (newResult != null)
            {
                yield return newResult;
            }
        }

        public TopicPartitionOffset? Pause(ConsumeResult<TKey, TValue> message)
        {
            var topicPartition = assignedTopicPartitions?.Where(k => k.Topic == message.Topic && k.Partition == message.Partition);
            if (topicPartition != null && topicPartition.Any())
            {
                consumer.Pause(topicPartition);
                return message.TopicPartitionOffset;
            }

            logger.LogWarning($"Pause discarded since no assigned TopicPartition for {message.TopicPartition}");
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
