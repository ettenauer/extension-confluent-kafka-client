using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Builder;
using Extension.Confluent.Kafka.Client.Configuration;
using Extension.Confluent.Kafka.Client.Consumer;
using Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy;
using Extension.Confluent.Kafka.Client.Health;
using Extension.Confluent.Kafka.Client.Metrics;
using Extension.Confluent.Kafka.Client.OffsetStore;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Tests.Consumer
{
    [TestFixture]
    public class BufferedConsumerTests
    {
        private BufferedConsumer<byte[], byte[]> consumer;
        private Mock<IConsumer<byte[], byte[]>> internalConsumerMock;
        private Mock<IConsumerBuilderWrapper<byte[], byte[]>> consumerBuilderMock;
        private Mock<IAdminClient> adminClientMock;
        private Mock<IAdminClientBuilderWrapper> adminClientBuilderMock;
        private Mock<IOffsetStore<byte[], byte[]>> offsetStoreMock;
        private Mock<IDispatcherStrategy<byte[], byte[]>> dispatcherStrategyMock;
        private Mock<IConsumeResultCallback<byte[], byte[]>> callbackMock;
        private Mock<IHealthStatusCallback> callbackHealthStatusMock;
        private Mock<IMetricsCallback> metricsCallbackMock;
        private BufferedConsumerConfig config;
        private Mock<ILogger> loggerMock;

        private Action<IConsumer<byte[], byte[]>, List<TopicPartition>> assignHandler;
        private Action<IConsumer<byte[], byte[]>, List<TopicPartitionOffset>> revokeHandler;

        [SetUp]
        public void SetUp()
        {
            consumerBuilderMock = new Mock<IConsumerBuilderWrapper<byte[], byte[]>>();
            consumerBuilderMock.Setup(_ => _.SetPartitionsAssignedHandler(It.IsAny<Action<IConsumer<byte[], byte[]>, List<TopicPartition>>>()))
                .Callback((Action<IConsumer<byte[], byte[]>, List<TopicPartition>> x) => assignHandler = x)
                .Returns(consumerBuilderMock.Object);
            consumerBuilderMock.Setup(_ => _.SetPartitionsRevokedHandler(It.IsAny<Action<IConsumer<byte[], byte[]>, List<TopicPartitionOffset>>>()))
                .Callback((Action<IConsumer<byte[], byte[]>, List<TopicPartitionOffset>> x) => revokeHandler = x)
                .Returns(consumerBuilderMock.Object);
            internalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            consumerBuilderMock.Setup(_ => _.Build()).Returns(internalConsumerMock.Object);
            adminClientBuilderMock = new Mock<IAdminClientBuilderWrapper>();
            adminClientMock = new Mock<IAdminClient>();
            adminClientBuilderMock.Setup(_ => _.Build()).Returns(adminClientMock.Object);
            offsetStoreMock = new Mock<IOffsetStore<byte[], byte[]>>();
            dispatcherStrategyMock = new Mock<IDispatcherStrategy<byte[], byte[]>>();
            callbackMock = new Mock<IConsumeResultCallback<byte[], byte[]>>();
            callbackHealthStatusMock = new Mock<IHealthStatusCallback>();
            metricsCallbackMock = new Mock<IMetricsCallback>();
            config = new BufferedConsumerConfig
            {
                TopicConfigs = new List<BufferedTopicConfig>
                    {
                        new BufferedTopicConfig { TopicName = "Test1"},
                    },
                PingIntervalInMilliseconds = 8000,
                BufferCommitIntervalInMilliseconds = 8000,
                CallbackResultCount = 5,
                BufferSharding = BufferSharding.Single,
                BackpressurePartitionPauseInMilliseconds = 1000
            };
            loggerMock = new Mock<ILogger>();
            consumer = new BufferedConsumer<byte[], byte[]>(
                consumerBuilderMock.Object,
                adminClientBuilderMock.Object,
                (c) => offsetStoreMock.Object,
                dispatcherStrategyMock.Object,
                callbackMock.Object,
                callbackHealthStatusMock.Object,
                metricsCallbackMock.Object,
                config,
                loggerMock.Object);
        }

        [Test]
        public void Ctor_ArgumentValidation_ThrowException()
        {
            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(null,
                adminClientBuilderMock.Object,
                (c) => offsetStoreMock.Object,
                dispatcherStrategyMock.Object,
                null,
                callbackHealthStatusMock.Object,
                null,
                config,
                loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                null,
                (c) => offsetStoreMock.Object,
                dispatcherStrategyMock.Object,
                null,
                callbackHealthStatusMock.Object,
                null,
                config,
                loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                adminClientBuilderMock.Object,
                null,
                dispatcherStrategyMock.Object,
                null,
                callbackHealthStatusMock.Object,
                null,
                config,
                loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                adminClientBuilderMock.Object,
                (c) => offsetStoreMock.Object,
                null,
                null,
                callbackHealthStatusMock.Object,
                null,
                config,
                loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                adminClientBuilderMock.Object,
                (c) => offsetStoreMock.Object,
                dispatcherStrategyMock.Object,
                null,
                null,
                null,
                config,
                loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                adminClientBuilderMock.Object,
                (c) => offsetStoreMock.Object,
                dispatcherStrategyMock.Object,
                null,
                callbackHealthStatusMock.Object,
                null,
                null,
                loggerMock.Object));


            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                adminClientBuilderMock.Object,
                (c) => offsetStoreMock.Object,
                dispatcherStrategyMock.Object,
                null,
                callbackHealthStatusMock.Object,
                null,
                config,
                null));
        }


        [Test]
        public void Ctor_DuplicatedTopics_ThrowException()
        {
            Assert.Throws<ArgumentException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                adminClientBuilderMock.Object,
                (c) => offsetStoreMock.Object,
                dispatcherStrategyMock.Object,
                null,
                callbackHealthStatusMock.Object,
                null,
                new BufferedConsumerConfig
                {
                    TopicConfigs = new List<BufferedTopicConfig>
                    {
                        new BufferedTopicConfig { TopicName = "Test1"},
                       new BufferedTopicConfig { TopicName = "Test1"}
                    }
                },
                loggerMock.Object));
        }

        [Test]
        public async Task AssignedHandler_SubscribedTopics_CheckMessageLoop()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            internalConsumerMock.Verify(c => c.Consume(It.IsAny<CancellationToken>()), Times.Never);

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            await Task.Delay(TimeSpan.FromSeconds(5));

            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeast(2));
            callbackHealthStatusMock.Verify(_ => _.OnHealthyConnection(), Times.Once);
            callbackHealthStatusMock.Verify(_ => _.OnMessageLoopPing(), Times.Once);
            callbackMock.Verify(_ => _.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce());
            offsetStoreMock.Verify(_ => _.FlushCommit(), Times.Once);

            consumer.Unsubcribe();
        }

        [Test]
        public async Task AssignedHandler_UnsubscribedTopics_CheckMessageLoop()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);

            //Note: messages are for Topic Test2 which is not part of config
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test2", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            internalConsumerMock.Verify(c => c.Consume(It.IsAny<CancellationToken>()), Times.Never);

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test2", 1) });

            await Task.Delay(TimeSpan.FromSeconds(5));

            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeast(2));
            callbackHealthStatusMock.Verify(_ => _.OnHealthyConnection(), Times.Once);
            callbackHealthStatusMock.Verify(_ => _.OnMessageLoopPing(), Times.Once);
            callbackMock.Verify(_ => _.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()), Times.Never());
            offsetStoreMock.Verify(_ => _.FlushCommit(), Times.Once);

            consumer.Unsubcribe();
        }

        [Test]
        public void RevokeHandler_Fired_CommittOffset()
        {
            var revokeList = new List<TopicPartitionOffset> { new TopicPartitionOffset("Test1", 1, 1) };

            revokeHandler.Invoke(internalConsumerMock.Object, revokeList);

            offsetStoreMock.Verify(_ => _.FlushCommit(), Times.Once);
            offsetStoreMock.Verify(_ => _.Clear(revokeList), Times.Once);
        }

        [Test]
        public async Task MessageLoop_ChannelsFull_PauseResumeTopicPartition()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });

            callbackMock.Setup(_ => _.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.Delay(TimeSpan.FromSeconds(5)));

            //Note: intial setup for new worker tasks
            dispatcherStrategyMock.SetupSequence(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            internalConsumerMock.Verify(c => c.Consume(It.IsAny<CancellationToken>()), Times.Never);

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            await Task.Delay(TimeSpan.FromSeconds(2));

            //Note: reuse same channel
            dispatcherStrategyMock.SetupSequence(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(false);

            await Task.Delay(TimeSpan.FromSeconds(5));

            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeastOnce());
            callbackHealthStatusMock.Verify(_ => _.OnHealthyConnection(), Times.Once);
            callbackHealthStatusMock.Verify(_ => _.OnMessageLoopPing(), Times.AtLeastOnce);
            callbackMock.Verify(_ => _.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce());
            offsetStoreMock.Verify(_ => _.FlushCommit(), Times.Once);
            internalConsumerMock.Verify(_ => _.Pause(It.IsAny<IEnumerable<TopicPartition>>()), Times.AtLeastOnce());
            internalConsumerMock.Verify(_ => _.Resume(It.IsAny<IEnumerable<TopicPartition>>()), Times.AtLeastOnce());

            consumer.Unsubcribe();
        }


        [Test]
        public async Task MessageLoop_Failure_ExpectOnUnhealthyConnection()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Throws(new Exception("Test Error"));
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            internalConsumerMock.Verify(c => c.Consume(It.IsAny<CancellationToken>()), Times.Never);

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            await Task.Delay(TimeSpan.FromSeconds(5));

            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeast(2));
            callbackHealthStatusMock.Verify(_ => _.OnHealthyConnection(), Times.AtLeast(2));
            adminClientMock.Verify(_ => _.GetMetadata(It.IsAny<string>(), It.IsAny<TimeSpan>()), Times.AtLeast(2));
            callbackHealthStatusMock.Verify(_ => _.OnMessageLoopPing(), Times.Never);
            callbackHealthStatusMock.Verify(_ => _.OnUnhealthyConnection(It.IsAny<Exception>()), Times.AtLeast(2));
            callbackMock.Verify(_ => _.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()), Times.Never());
            offsetStoreMock.Verify(_ => _.FlushCommit(), Times.Never);

            consumer.Unsubcribe();
        }
    }
}
