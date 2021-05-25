using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Extension.Confluent.Kafka.Client.Consumer.Builder
{
    internal interface IConsumerBuilderWrapper<TKey, TValue>
    {
        IConsumerBuilderWrapper<TKey, TValue> SetPartitionsAssignedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartition>> partitionsAssingedHandler);
        IConsumerBuilderWrapper<TKey, TValue> SetPartitionsRevokedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionsRevokedHandler);
        IConsumer<TKey, TValue> Build();
    }
}
