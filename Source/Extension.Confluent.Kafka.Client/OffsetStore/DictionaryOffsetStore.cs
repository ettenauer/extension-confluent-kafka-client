using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Extension.Confluent.Kafka.Client.OffsetStore
{
    internal class DictionaryOffsetStore<TKey, TValue> : IOffsetStore<TKey,TValue>
    {
        private readonly IConsumer<TKey, TValue> consumer;
        //Note: required to sync clear with other operations
        private readonly ReaderWriterLockSlim slimLock = new ReaderWriterLockSlim();

        private ConcurrentDictionary<TopicPartition, ConsumeResult<TKey, TValue>> latestMessageByTopicPartition;

        public DictionaryOffsetStore(IConsumer<TKey, TValue> consumer)
        {
            this.consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            latestMessageByTopicPartition = new ConcurrentDictionary<TopicPartition, ConsumeResult<TKey, TValue>>();
        }

        public void Store(ReadOnlyMemory<ConsumeResult<TKey, TValue>> results)
        {
        }

        public void Complete(ReadOnlyMemory<ConsumeResult<TKey, TValue>> results)
        {
            slimLock.EnterReadLock();
            try
            {
                for (int i = 0; i < results.Length; i++)
                {
                    latestMessageByTopicPartition[results.Span[i].TopicPartition] = results.Span[i];
                }
            }
            finally
            {
                slimLock.ExitReadLock();
            }
        }

        public IEnumerable<TopicPartitionOffset> FlushCommit()
        {
            var committableOffsets = new List<TopicPartitionOffset>();

            slimLock.EnterReadLock();
            try
            {
                foreach (var topicPartition in latestMessageByTopicPartition.Keys.ToList())
                {
                    if (latestMessageByTopicPartition.TryRemove(topicPartition, out var message))
                    {
                        // Note: The committed offset should always be the offset of the next message that your application will read. Thus, when calling commitSync(offsets) you should add one to the offset of the last message processed. 
                        // (see https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)
                        committableOffsets.Add(new TopicPartitionOffset(message.TopicPartition, message.Offset + 1));
                    }
                }
            }
            finally
            {
                slimLock.ExitReadLock();
            }

            if (committableOffsets.Count > 0)
            {
                consumer.Commit(committableOffsets);
            }

            return committableOffsets;
        }

        public void Clear(IEnumerable<TopicPartitionOffset>? obsoleteTopicPartitions = null)
        {
            slimLock.EnterWriteLock();
            try
            {
                if (obsoleteTopicPartitions == null)
                {
                    latestMessageByTopicPartition = new ConcurrentDictionary<TopicPartition, ConsumeResult<TKey, TValue>>();
                }
                else
                {
                    foreach(var obsoleteTopicPartiton in obsoleteTopicPartitions)
                    {
                        latestMessageByTopicPartition.TryRemove(obsoleteTopicPartiton.TopicPartition, out _);
                    }
                }
            }
            finally
            {
                slimLock.ExitWriteLock();
            }
        }

        public void Dispose()
        {
            slimLock.Dispose();
        }
    }
}
