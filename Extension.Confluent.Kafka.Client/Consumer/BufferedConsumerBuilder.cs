﻿using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Configuration;
using Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy;
using Extension.Confluent.Kafka.Client.Health;
using Extension.Confluent.Kafka.Client.Metrics;
using Extension.Confluent.Kafka.Client.OffsetStore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Extension.Confluent.Kafka.Client.Consumer
{
    public class BufferedConsumerBuilder<TKey, TValue>
    {
        private ConsumerBuilder<TKey, TValue>? consumerBuilder;
        private Func<IAdminClient>? createAdminClientFunc;
        private IHealthStatusCallback? healthStatusCallback;
        private IConsumeResultCallback<TKey, TValue>? callback;
        private IMetricsCallback? metricsCallback;
        private Func<ConsumeResult<TKey, TValue>, long>? getChannelIdFunc;
        private Action<IConsumer<TKey, TValue>, List<TopicPartition>>? partitionsAssingedHandler;
        private Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>>? partitionsRevokedHandler;
        private BufferedConsumerConfig? configuration;
        private ILogger? logger;

        public BufferedConsumerBuilder<TKey, TValue> SetConsumerBuilder(ConsumerBuilder<TKey, TValue> consumerBuilder)
        {
            this.consumerBuilder = consumerBuilder ?? throw new ArgumentNullException(nameof(consumerBuilder));
            return this;
        }

        public BufferedConsumerBuilder<TKey, TValue> SetAdminClient(Func<IAdminClient> createAdminClientFunc)
        {
            this.createAdminClientFunc = createAdminClientFunc ?? throw new ArgumentNullException(nameof(createAdminClientFunc));
            return this;
        }

        public BufferedConsumerBuilder<TKey, TValue> SetHealthStatusCallback(IHealthStatusCallback healthStatusCallback)
        {
            this.healthStatusCallback = healthStatusCallback;
            return this;
        }

        public BufferedConsumerBuilder<TKey, TValue> SetCallback(IConsumeResultCallback<TKey, TValue> callback)
        {
            this.callback = callback;
            return this;
        }

        public BufferedConsumerBuilder<TKey, TValue> SetMetricsCallback(IMetricsCallback metricsCallback)
        {
            this.metricsCallback = metricsCallback;
            return this;
        }

        public BufferedConsumerBuilder<TKey, TValue> SetConfiguration(BufferedConsumerConfig configuration)
        {
            this.configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            return this;
        }

        public BufferedConsumerBuilder<TKey, TValue> SetChannelIdFunc(Func<ConsumeResult<TKey, TValue>, long> getChannelIdFunc)
        {
            this.getChannelIdFunc = getChannelIdFunc ?? throw new ArgumentNullException(nameof(getChannelIdFunc));
            return this;
        }

        public BufferedConsumerBuilder<TKey, TValue> SetPartitionsAssignedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartition>> partitionsAssingedHandler)
        {
            this.partitionsAssingedHandler = partitionsAssingedHandler ?? throw new ArgumentNullException(nameof(partitionsAssingedHandler));
            return this;
        }

        public BufferedConsumerBuilder<TKey, TValue> SetPartitionsRevokedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionsRevokedHandler)
        {
            this.partitionsRevokedHandler = partitionsRevokedHandler ?? throw new ArgumentNullException(nameof(partitionsRevokedHandler));
            return this;
        }

        public BufferedConsumerBuilder<TKey, TValue> SetLogger(ILogger logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            return this;
        }

        public IBufferedConsumer<TKey, TValue> Build()
        {
            if (consumerBuilder == null) throw new ArgumentException($"{nameof(SetConsumerBuilder)} must be called");
            if (createAdminClientFunc == null) throw new ArgumentException($"{nameof(SetAdminClient)} must be called");
            if (configuration == null) throw new ArgumentException($"{nameof(SetConfiguration)} must be called");
            if (logger == null) throw new ArgumentException($"{nameof(SetLogger)} must be called");

            return new BufferedConsumer<TKey, TValue>(new ConsumerBuilderWrapper<TKey, TValue>(consumerBuilder, partitionsAssingedHandler, partitionsRevokedHandler),
                createAdminClientFunc(),
                (c) => CreateOffsetStore(c, configuration),
                CreateDispatcher(configuration),
                callback,
                healthStatusCallback,
                metricsCallback,
                configuration,
                logger);
        }

        private IOffsetStore<TKey, TValue> CreateOffsetStore(IConsumer<TKey, TValue> internalConsumer, BufferedConsumerConfig config)
        {
            switch (config.BufferSharding)
            { 
                case BufferSharding.Task:
                    //Note: set initial capacity to 1 offset every millisecond
                    return new HeapOffsetStore<TKey, TValue>(internalConsumer, new OffsetStoreConfig { DefaultHeapSize = config.BufferCommitIntervalInMilliseconds });
                default:
                    return new DictionaryOffsetStore<TKey, TValue>(internalConsumer);
            }
        }

        private IDispatcherStrategy<TKey, TValue> CreateDispatcher(BufferedConsumerConfig config)
        {
            byte priorityChannelCount = Convert.ToByte(config.TopicConfigs.Select(_ => _.Priority).Distinct().Count());

            switch (config.BufferSharding)
            {
                case BufferSharding.Task:
                    var taskMaxCount = config.BufferMaxTaskCount ?? throw new ArgumentException($"{config.BufferMaxTaskCount} needs to be set when {BufferSharding.Task} is set");
                    var func = getChannelIdFunc ?? throw new ArgumentException($"{nameof(SetChannelIdFunc)} needs to be configured when {BufferSharding.Task} is set");
                    return new TaskStrategy<TKey, TValue>(func, config.BufferSizePerChannel, priorityChannelCount, taskMaxCount);
                case BufferSharding.Parition:
                    return new PartitionStrategy<TKey, TValue>(config.BufferSizePerChannel, priorityChannelCount);
                case BufferSharding.Single:
                    return new SingleStrategy<TKey, TValue>(config.BufferSizePerChannel, priorityChannelCount);
                default:
                    throw new ArgumentException($"BufferSharding {config?.BufferSharding} not supported");
            }
        }
    }
}
