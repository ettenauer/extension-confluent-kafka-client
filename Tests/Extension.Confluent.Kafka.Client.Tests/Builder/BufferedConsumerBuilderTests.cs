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
        private BufferedConsumerConfig config;

        [SetUp]
        public void SetUp()
        {
            config = new BufferedConsumerConfig
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
        }

        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetConsumerBuilder))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetLogger))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetAdminBuilder))]
        public void Build_MandatorySetupNotExists_ThrowException(string missingDependency)
        {
            var mandatoryDependencies = new Dictionary<string, Func<BufferedConsumerBuilderWithMock, BufferedConsumerBuilderWithMock>>()
            {
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetConsumerBuilder), (b) => (BufferedConsumerBuilderWithMock)b.SetConsumerBuilder(new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig()))},
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetAdminBuilder), (b) => (BufferedConsumerBuilderWithMock)b.SetAdminBuilder(new AdminClientBuilder(new ConsumerConfig())) },
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetLogger), (b) => (BufferedConsumerBuilderWithMock)b.SetLogger(new Mock<ILogger>().Object)}
            };

            mandatoryDependencies[missingDependency] = (b) => b;

            var builder = new BufferedConsumerBuilderWithMock(config);

            foreach(var dependency in mandatoryDependencies)
            {
                builder = dependency.Value(builder);
            }

            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetConsumerBuilder))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetAdminBuilder))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetLogger))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetCallback))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetChannelIdFunc))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetHealthStatusCallback))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetMetricsCallback))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetPartitionsAssignedHandler))]
        [TestCase(nameof(BufferedConsumerBuilder<byte[], byte[]>.SetPartitionsRevokedHandler))]
        public void Build_SetupInvalidArgument_ThrowException(string setupMethod)
        {
            var invalidArgumentSetup = new Dictionary<string, Func<BufferedConsumerBuilderWithMock, BufferedConsumerBuilderWithMock>>()
            {
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetConsumerBuilder), (b) => (BufferedConsumerBuilderWithMock)b.SetConsumerBuilder(null)},
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetAdminBuilder), (b) => (BufferedConsumerBuilderWithMock)b.SetAdminBuilder(null) },
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetLogger), (b) => (BufferedConsumerBuilderWithMock)b.SetLogger(null)},
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetCallback), (b) => (BufferedConsumerBuilderWithMock)b.SetCallback(null)},
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetChannelIdFunc), (b) => (BufferedConsumerBuilderWithMock)b.SetChannelIdFunc(null)},
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetHealthStatusCallback), (b) => (BufferedConsumerBuilderWithMock)b.SetHealthStatusCallback(null)},
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetMetricsCallback), (b) => (BufferedConsumerBuilderWithMock)b.SetMetricsCallback(null)},
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetPartitionsAssignedHandler), (b) => (BufferedConsumerBuilderWithMock)b.SetPartitionsAssignedHandler(null)},
                { nameof(BufferedConsumerBuilder<byte[], byte[]>.SetPartitionsRevokedHandler), (b) => (BufferedConsumerBuilderWithMock)b.SetPartitionsRevokedHandler(null)},
            };

            var builder = new BufferedConsumerBuilderWithMock(config);

            Assert.Throws<ArgumentNullException>(() => invalidArgumentSetup[setupMethod](builder));
        }

        [Test]
        public void Build_MandatorySetupExists_NewConsumer()
        {       
            var builder = new BufferedConsumerBuilderWithMock(config)
                .SetAdminBuilder(new AdminClientBuilder(new ConsumerConfig()))
                .SetConsumerBuilder(new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig()))
                .SetLogger(new Mock<ILogger>().Object)
                .Build();

            Assert.That(builder, Is.Not.Null);         
        }

        [TestCase(nameof(BufferedConsumerConfig.BufferMaxTaskCount))]
        [TestCase(nameof(BufferedConsumerBuilderWithMock.SetChannelIdFunc))]
        public void Build_IncorrectSetupShardingTask_ThrowException(string setup)
        {
            var channelIdSetupFunc = new Func<BufferedConsumerBuilderWithMock, BufferedConsumerBuilderWithMock>(
                c => (BufferedConsumerBuilderWithMock)c.SetChannelIdFunc(x => x.Partition)
                );

            switch (setup)
            {
                case nameof(BufferedConsumerConfig.BufferMaxTaskCount):
                    config = new BufferedConsumerConfig
                    {
                        BufferSizePerChannel = 1,
                        BufferMaxTaskCount = 0,
                        BufferSharding = BufferSharding.Task,
                        TopicConfigs = new List<BufferedTopicConfig>()
                        {
                            new BufferedTopicConfig
                            {
                                TopicName = "Test"
                            }
                        }
                    };
                    break;
                case nameof(BufferedConsumerBuilderWithMock.SetChannelIdFunc):
                    config = new BufferedConsumerConfig
                    {
                        BufferSizePerChannel = 1,
                        BufferMaxTaskCount = 2,
                        BufferSharding = BufferSharding.Task,
                        TopicConfigs = new List<BufferedTopicConfig>()
                        {
                            new BufferedTopicConfig
                            {
                                TopicName = "Test"
                            }
                        }
                    };
                    channelIdSetupFunc = new Func<BufferedConsumerBuilderWithMock, BufferedConsumerBuilderWithMock>(c => c);
                    break;
            }
       
            var builder = new BufferedConsumerBuilderWithMock(config)
                .SetAdminBuilder(new AdminClientBuilder(new ConsumerConfig()))
                .SetConsumerBuilder(new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig()))
                .SetLogger(new Mock<ILogger>().Object);

            builder = channelIdSetupFunc((BufferedConsumerBuilderWithMock) builder);

            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [TestCase(BufferSharding.Parition)]
        [TestCase(BufferSharding.Task)]
        [TestCase(BufferSharding.Single)]
        public void Build_ConfiguredSharding_ExpectedDispatcherStrategy(BufferSharding sharding)
        {
            config = new BufferedConsumerConfig
            {
                BufferSizePerChannel = 1,
                BufferMaxTaskCount = 2,
                BufferSharding = sharding,
                TopicConfigs = new List<BufferedTopicConfig>()
                        {
                            new BufferedTopicConfig
                            {
                                TopicName = "Test"
                            }
                        }
            };

            var builder = (BufferedConsumerBuilderWithMock) new BufferedConsumerBuilderWithMock(config)
                .SetAdminBuilder(new AdminClientBuilder(new ConsumerConfig()))
                .SetConsumerBuilder(new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig()))
                .SetChannelIdFunc(p => p.Partition)
                .SetLogger(new Mock<ILogger>().Object);

            builder.Build();

           switch (config.BufferSharding)
            {
                case BufferSharding.Parition:
                    Assert.That(builder.DispatcherStrategy, Is.AssignableTo(typeof(PartitionStrategy<byte[], byte[]>)));
                    break;
                case BufferSharding.Single:
                    Assert.That(builder.DispatcherStrategy, Is.AssignableTo(typeof(SingleStrategy<byte[], byte[]>)));
                    break;
                case BufferSharding.Task:
                    Assert.That(builder.DispatcherStrategy, Is.AssignableTo(typeof(TaskStrategy<byte[], byte[]>)));
                    break;
                default:
                    Assert.Fail("missing dispatcher strategy");
                    break;
            }
        }

        [TestCase(BufferSharding.Parition)]
        [TestCase(BufferSharding.Task)]
        [TestCase(BufferSharding.Single)]
        public void Build_ConfiguredSharding_ExpectedOffsetStore(BufferSharding sharding)
        {
            config = new BufferedConsumerConfig
            {
                BufferSizePerChannel = 1,
                BufferMaxTaskCount = 2,
                BufferSharding = sharding,
                TopicConfigs = new List<BufferedTopicConfig>()
                        {
                            new BufferedTopicConfig
                            {
                                TopicName = "Test"
                            }
                        }
            };

            var builder = (BufferedConsumerBuilderWithMock)new BufferedConsumerBuilderWithMock(config)
                .SetAdminBuilder(new AdminClientBuilder(new ConsumerConfig()))
                .SetConsumerBuilder(new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig()))
                .SetChannelIdFunc(p => p.Partition)
                .SetLogger(new Mock<ILogger>().Object);

            builder.Build();

            switch (config.BufferSharding)
            {
                case BufferSharding.Parition:
                case BufferSharding.Single:
                    Assert.That(builder.OffsetStore, Is.AssignableTo(typeof(DictionaryOffsetStore<byte[],byte[]>)));
                    break;
                case BufferSharding.Task:
                    Assert.That(builder.OffsetStore, Is.AssignableTo(typeof(HeapOffsetStore<byte[], byte[]>)));
                    break;
                default:
                    Assert.Fail("missing offset store");
                    break;
            }
        }

        private class BufferedConsumerBuilderWithMock : BufferedConsumerBuilder<byte[], byte[]>
        {
            public IDispatcherStrategy<byte[], byte[]> DispatcherStrategy { get; set; }

            public IOffsetStore<byte[], byte[]> OffsetStore { get; set; }

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
                DispatcherStrategy = dispatcherStrategy;
                OffsetStore = createOffsetStoreFunc(new Mock<IConsumer<byte[], byte[]>>().Object);
                return new Mock<IBufferedConsumer<byte[], byte[]>>().Object;
            }
        }
    }
}
