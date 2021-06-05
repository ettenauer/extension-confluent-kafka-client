using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Extension.Confluent.Kafka.Client.Builder
{
    /// <summary>
    /// Required in order to do unit tests for BufferedConsumer
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    [ExcludeFromCodeCoverage]
    internal class ConsumerBuilderWrapper<TKey, TValue> : IConsumerBuilderWrapper<TKey, TValue>
    {
        private readonly ConsumerBuilder<TKey, TValue> consumerBuilder;
        private readonly Action<IConsumer<TKey, TValue>, List<TopicPartition>>? sourcePartitionsAssignedHandler;
        private readonly Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>>? sourcePartitionsRevokedHandler;

        public ConsumerBuilderWrapper(ConsumerBuilder<TKey, TValue> consumerBuilder,
            Action<IConsumer<TKey, TValue>, List<TopicPartition>>? sourcePartitionsAssignedHandler,
            Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>>? sourcePartitionsRevokedHandler)
        {
            this.consumerBuilder = consumerBuilder ?? throw new ArgumentNullException(nameof(consumerBuilder));
            this.sourcePartitionsAssignedHandler = sourcePartitionsAssignedHandler;
            this.sourcePartitionsRevokedHandler = sourcePartitionsRevokedHandler;
        }

        public IConsumer<TKey, TValue> Build()
        {
            return this.consumerBuilder.Build();
        }

        public IConsumerBuilderWrapper<TKey, TValue> SetPartitionsAssignedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartition>> partitionsAssingedHandler)
        {
            this.consumerBuilder.SetPartitionsAssignedHandler((c, list) =>
            {
                partitionsAssingedHandler.Invoke(c, list);
                sourcePartitionsAssignedHandler?.Invoke(c, list);
            });

            return this;
        }

        public IConsumerBuilderWrapper<TKey, TValue> SetPartitionsRevokedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionsRevokedHandler)
        {
            this.consumerBuilder.SetPartitionsRevokedHandler((c, list) =>
            {
                partitionsRevokedHandler.Invoke(c, list);
                sourcePartitionsRevokedHandler?.Invoke(c, list);
            });

            return this;
        }
    }
}
