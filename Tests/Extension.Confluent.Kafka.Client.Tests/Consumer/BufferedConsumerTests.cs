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
        private Mock<IHealthStatusCallback> healthStatusCallbackMock;
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
            healthStatusCallbackMock = new Mock<IHealthStatusCallback>();
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
                healthStatusCallbackMock.Object,
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
                null,
                null,
                config,
                loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                null,
                (c) => offsetStoreMock.Object,
                dispatcherStrategyMock.Object,
                null,
                null,
                null,
                config,
                loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                adminClientBuilderMock.Object,
                null,
                dispatcherStrategyMock.Object,
                null,
                null,
                null,
                config,
                loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                adminClientBuilderMock.Object,
                (c) => offsetStoreMock.Object,
                null,
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
                null,
                null,
                null,
                loggerMock.Object));


            Assert.Throws<ArgumentNullException>(() => new BufferedConsumer<byte[], byte[]>(consumerBuilderMock.Object,
                adminClientBuilderMock.Object,
                (c) => offsetStoreMock.Object,
                dispatcherStrategyMock.Object,
                null,
                null,
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
                healthStatusCallbackMock.Object,
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
        public async Task Subscribe_SubscribedTopics_CheckMessageLoop()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            await Task.Delay(TimeSpan.FromSeconds(2));

            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeast(2));

            //Note: health check is triggered
            healthStatusCallbackMock.Verify(_ => _.OnHealthyConnection(), Times.Once);
            healthStatusCallbackMock.Verify(_ => _.OnMessageLoopPing(), Times.Once);

            //Note: callback is triggered
            callbackMock.Verify(_ => _.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce());
            metricsCallbackMock.Verify(_ => _.RecordSuccess(It.IsAny<int>(), It.IsAny<TimeSpan>()), Times.AtLeastOnce);

            //Note: verify if offse store is properly used
            offsetStoreMock.Verify(_ => _.Store(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>()), Times.AtLeastOnce);
            offsetStoreMock.Verify(_ => _.Complete(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>()), Times.AtLeastOnce);
            offsetStoreMock.Verify(_ => _.FlushCommit(), Times.Once);

            consumer.Unsubscribe();
        }

        [Test]
        public async Task Subscribe_OptionalCallbackNotSet_CheckConsumedMessages()
        {
            consumer = new BufferedConsumer<byte[], byte[]>(
                    consumerBuilderMock.Object,
                    adminClientBuilderMock.Object,
                    (c) => offsetStoreMock.Object,
                    dispatcherStrategyMock.Object,
                    null, // callback not set
                    null, // callback not set
                    null, // callback not set
                    config,
                    loggerMock.Object);

            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            await Task.Delay(TimeSpan.FromSeconds(2));

            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeast(2));

            //Note: verify if offse store is properly used
            offsetStoreMock.Verify(_ => _.Store(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>()), Times.AtLeastOnce);
            offsetStoreMock.Verify(_ => _.Complete(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>()), Times.AtLeastOnce);
            offsetStoreMock.Verify(_ => _.FlushCommit(), Times.Once);

            consumer.Unsubscribe();
        }

        [Test]
        public async Task Subscribe_NoAssignedTopicPartitions_CheckMessageLoop()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);

            //Note: messages are for Topic Test2 which is not part of config
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test2", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition>());

            await Task.Delay(TimeSpan.FromSeconds(2));

            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeast(2));

            //Note: health check is triggered
            healthStatusCallbackMock.Verify(_ => _.OnHealthyConnection(), Times.Once);
            healthStatusCallbackMock.Verify(_ => _.OnMessageLoopPing(), Times.Once);

            //Note: should not be called since not active TopicParitions
            dispatcherStrategyMock.Verify(_ => _.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel), Times.Never);
            callbackMock.Verify(_ => _.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()), Times.Never());

            consumer.Unsubscribe();
        }

        [Test]
        public async Task Subscribe_MultipleCalls_CheckOnMessageLoopCancelled()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            await Task.Delay(TimeSpan.FromSeconds(2));

            consumer.Subscribe();

            await Task.Delay(TimeSpan.FromSeconds(2));

            //Note: cancelled by the second subscribe call
            healthStatusCallbackMock.Verify(_ => _.OnMessageLoopCancelled(It.IsAny<string>()), Times.Once);

            consumer.Unsubscribe();

            await Task.Delay(TimeSpan.FromSeconds(2));

            //Note: verify cancellation
            healthStatusCallbackMock.Verify(_ => _.OnMessageLoopCancelled(It.IsAny<string>()), Times.Exactly(2));
            internalConsumerMock.Verify(_ => _.Subscribe(It.IsAny<IEnumerable<string>>()), Times.AtLeast(2));
        }

        [Test]
        public async Task Unsubscribe_MultipleCalls_CheckOnMessageLoopCancelled()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            consumer.Unsubscribe();

            consumer.Unsubscribe();

            await Task.Delay(TimeSpan.FromSeconds(2));

            //Note: verify cancellation
            healthStatusCallbackMock.Verify(_ => _.OnMessageLoopCancelled(It.IsAny<string>()), Times.Once);
            internalConsumerMock.Verify(_ => _.Unsubscribe(), Times.AtLeast(2));
        }

        [Test]
        public async Task Dispose_Destory_CheckOnMessageLoopCancelled()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            consumer.Dispose();

            await Task.Delay(TimeSpan.FromSeconds(2));

            //Note: verify cancellation
            healthStatusCallbackMock.Verify(_ => _.OnMessageLoopCancelled(It.IsAny<string>()), Times.Once);
            internalConsumerMock.Verify(_ => _.Unsubscribe(), Times.Once);
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
        public async Task AssignHandler_Fired_CancelledNotRequiredWorker()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            await Task.Delay(TimeSpan.FromSeconds(2));

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition>());

            await Task.Delay(TimeSpan.FromSeconds(2));

            //Note: verify cancellation
            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeast(2));
            healthStatusCallbackMock.Verify(_ => _.OnWorkerTaskCancelled(1, It.IsAny<string>()), Times.AtLeastOnce);

            consumer.Unsubscribe();
        }

        [Test]
        public async Task MessageLoop_ChannelsFull_PauseResumeTopicPartition()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });

            callbackMock.Setup(_ => _.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.Delay(TimeSpan.FromMilliseconds(500)));

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

            await Task.Delay(TimeSpan.FromSeconds(2));

            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeastOnce());
            callbackMock.Verify(_ => _.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce());
            internalConsumerMock.Verify(_ => _.Pause(It.IsAny<IEnumerable<TopicPartition>>()), Times.AtLeastOnce());
            internalConsumerMock.Verify(_ => _.Resume(It.IsAny<IEnumerable<TopicPartition>>()), Times.AtLeastOnce());

            consumer.Unsubscribe();
        }

        [Test]
        public async Task MessageLoop_ConsumeFailure_ExpectOnUnhealthyConnection()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Throws(new Exception("Test Error"));
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            consumer.Subscribe();

            internalConsumerMock.Verify(c => c.Consume(It.IsAny<CancellationToken>()), Times.Never);

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            await Task.Delay(TimeSpan.FromSeconds(2));

            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeast(2));
            healthStatusCallbackMock.Verify(_ => _.OnHealthyConnection(), Times.AtLeast(2));
            adminClientMock.Verify(_ => _.GetMetadata(It.IsAny<string>(), It.IsAny<TimeSpan>()), Times.AtLeast(2));
            healthStatusCallbackMock.Verify(_ => _.OnMessageLoopPing(), Times.Never);
            healthStatusCallbackMock.Verify(_ => _.OnUnhealthyConnection(It.IsAny<Exception>()), Times.AtLeast(2));

            consumer.Unsubscribe();
        }

        [Test]
        public async Task MessageLoop_ConnectionFailed_ExpectOnUnhealthyConnection()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            adminClientMock.Setup(_ => _.GetMetadata(It.IsAny<string>(), It.IsAny<TimeSpan>())).Throws(new Exception("Test Error"));

            consumer.Subscribe();

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            await Task.Delay(TimeSpan.FromSeconds(2));

            internalConsumerMock.Verify(_ => _.Consume(It.IsAny<TimeSpan>()), Times.AtLeast(2));

            //Note: health check is triggered
            healthStatusCallbackMock.Verify(_ => _.OnHealthyConnection(), Times.Never);
            healthStatusCallbackMock.Verify(_ => _.OnUnhealthyConnection(It.IsAny<Exception>()), Times.AtLeast(2));
            healthStatusCallbackMock.Verify(_ => _.OnMessageLoopPing(), Times.Once);

            consumer.Unsubscribe();
        }

        [Test]
        public async Task MessageLoop_CallbackError_RecordFailureAndOffsetCompelete()
        {
            IConsumeResultChannel<byte[], byte[]> channel = new ConsumeResultChannel<byte[], byte[]>(1, 2, 1);
            internalConsumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("Test1", 1, 1) });
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            callbackMock.Setup(_ => _.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()))
                .Throws(new Exception("Test Error"));

            consumer.Subscribe();

            internalConsumerMock.Verify(c => c.Consume(It.IsAny<CancellationToken>()), Times.Never);

            assignHandler.Invoke(internalConsumerMock.Object, new List<TopicPartition> { new TopicPartition("Test1", 1) });

            await Task.Delay(TimeSpan.FromSeconds(1));

            //Note: health check is triggered
            healthStatusCallbackMock.Verify(_ => _.OnHealthyConnection(), Times.Once);
            healthStatusCallbackMock.Verify(_ => _.OnMessageLoopPing(), Times.Once);

            //Note: callback is triggered
            metricsCallbackMock.Verify(_ => _.RecordFailure(It.IsAny<int>(), It.IsAny<TimeSpan>()), Times.AtLeastOnce());

            //Note: verify if offse store is properly used
            offsetStoreMock.Verify(_ => _.Store(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>()), Times.AtLeastOnce);
            offsetStoreMock.Verify(_ => _.Complete(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>()), Times.AtLeastOnce);
            offsetStoreMock.Verify(_ => _.FlushCommit(), Times.Once);

            consumer.Unsubscribe();
        }
    }
}
