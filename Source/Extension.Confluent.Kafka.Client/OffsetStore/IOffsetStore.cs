using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Extension.Confluent.Kafka.Client.OffsetStore
{
    public interface IOffsetStore<TKey, TValue> : IDisposable
    {
        void Store(ReadOnlyMemory<ConsumeResult<TKey, TValue>> results);
        void Complete(ReadOnlyMemory<ConsumeResult<TKey, TValue>> results);
        IEnumerable<TopicPartitionOffset> FlushCommit();
        void Clear(IEnumerable<TopicPartitionOffset>? obsoleteTopicParitions = null);
    }
}
