using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Builder;
using Extension.Confluent.Kafka.Client.Configuration;
using Extension.Confluent.Kafka.Client.Consumer;
using Extension.Confluent.Kafka.Client.Consumer.Builder;
using Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy;
using Extension.Confluent.Kafka.Client.Health;
using Extension.Confluent.Kafka.Client.Metrics;
using Extension.Confluent.Kafka.Client.OffsetStore;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Extension.Confluent.Kafka.Client.Tests.Builder
{
    [TestFixture]
    public class BufferedConsumerBuilderTests
    {

        [SetUp]
        public void SetUp()
        {

        }

        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetConsumerBuilder))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetLogger))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetAdminBuilder))]
        public void Build_MandatoryDependenciesMissing_ThrowException(string missingDependency)
        {
            var mandatoryDependencies = new Dictionary<string, Func<BufferedConsumerBuilderWithMock, BufferedConsumerBuilderWithMock>>()
            {
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetConsumerBuilder), (b) => (BufferedConsumerBuilderWithMock)b.SetConsumerBuilder(new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig()))},
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetAdminBuilder), (b) => (BufferedConsumerBuilderWithMock)b.SetAdminBuilder(new AdminClientBuilder(new ConsumerConfig())) },
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetLogger), (b) => (BufferedConsumerBuilderWithMock)b.SetLogger(new Mock<ILogger>().Object)}
            };

            mandatoryDependencies[missingDependency] = (b) => b;

            var config = new BufferedConsumerConfig
            {
                BufferSizePerChannel = 1,
                BufferSharding = BufferSharding.Single,
                TopicConfigs = new List<BufferedTopicConfig>()
                {
                    new BufferedTopicConfig
                    {
                        TopicName = "Test"
                    }
                }
            };

            var builder = new BufferedConsumerBuilderWithMock(config);

            foreach(var dependency in mandatoryDependencies)
            {
                builder = dependency.Value(builder);
            }

            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void Build_MandatoryDependenciesExists_NewConsumer()
        {
            var config = new BufferedConsumerConfig
            {
                BufferSizePerChannel = 1,
                BufferSharding = BufferSharding.Single,
                TopicConfigs = new List<BufferedTopicConfig>()
                {
                    new BufferedTopicConfig
                    {
                        TopicName = "Test"
                    }
                }
            };

            var builder = new BufferedConsumerBuilderWithMock(config)
                .SetAdminBuilder(new AdminClientBuilder(new ConsumerConfig()))
                .SetConsumerBuilder(new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig()))
                .SetLogger(new Mock<ILogger>().Object)
                .Build();

            Assert.That(builder, Is.Not.Null);         
        }

        [Test]
        public void Build_WrongBufferShardingTaskSetup_Exception()
        {
     
        }

        [Test]
        public void Build_BufferSharding_ExpectedOffsetStore()
        {

        }

        [Test]
        public void Build_BufferSharding_ExpectedDispatchStrategy()
        {

        }

        private class BufferedConsumerBuilderWithMock : BufferedConsumerBuilder<byte[], byte[]>
        {
            public BufferedConsumerBuilderWithMock(BufferedConsumerConfig config) : base(config)
            {
            }

            //Note: create mock to allow unit testing
            protected override IBufferedConsumer<byte[], byte[]> CreateConsumer(IConsumerBuilderWrapper<byte[], byte[]> consumerBuilder,
            IAdminClientBuilderWrapper adminClientBuilder,
            Func<IConsumer<byte[], byte[]>, IOffsetStore<byte[], byte[]>> createOffsetStoreFunc,
            IDispatcherStrategy<byte[], byte[]> dispatcherStrategy,
#pragma warning disable CS8632
            IConsumeResultCallback<byte[], byte[]>? callback,
            IHealthStatusCallback? healthStatusCallback,
            IMetricsCallback? metricsCallback,
#pragma warning restore CS8632
            BufferedConsumerConfig config,
            ILogger logger)
            {
                return new Mock<IBufferedConsumer<byte[], byte[]>>().Object;
            }
        }
    }
}
