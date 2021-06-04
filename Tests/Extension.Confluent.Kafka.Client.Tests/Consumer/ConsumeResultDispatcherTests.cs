using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Consumer;
using Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy;
using Extension.Confluent.Kafka.Client.Health;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Tests.Consumer
{
    [TestFixture]
    public class ConsumeResultDispatcherTests
    {
        private ConsumeResultDispatcher<byte[], byte[]> dispatcher;
        private Mock<IConsumeResultCallback<byte[], byte[]>> callbackMock;
        private Mock<IDispatcherStrategy<byte[], byte[]>> dispatcherStrategyMock;
        private ConsumeResultChannelWorkerConfig config;
        private Mock<IConsumeResultChannel<byte[], byte[]>> channelMock;
        private ConsumeResult<byte[], byte[]> fakeResult;
        private Mock<ILogger> loggerMock;
        private Mock<IHealthStatusCallback> healthStatusCallbackMock;


        [SetUp]
        public void SetUp()
        {
            callbackMock = new Mock<IConsumeResultCallback<byte[], byte[]>>();
            config = new ConsumeResultChannelWorkerConfig { CallbackResultCount = 2 };
            channelMock = new Mock<IConsumeResultChannel<byte[], byte[]>>();
            fakeResult = new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>
                {
                    Headers = new Headers()
                }
            };
            dispatcherStrategyMock = new Mock<IDispatcherStrategy<byte[], byte[]>>();
            loggerMock = new Mock<ILogger>();
            healthStatusCallbackMock = new Mock<IHealthStatusCallback>();
            dispatcher = new ConsumeResultDispatcher<byte[], byte[]>(
                callbackMock.Object,
                dispatcherStrategyMock.Object,
                healthStatusCallbackMock.Object,
                config,
                loggerMock.Object);
        }

        [Test]
        public void Ctor_ArgumentValidation_ThrowException()
        {
            Assert.Throws<ArgumentNullException>(() => new ConsumeResultDispatcher<byte[], byte[]>(null, dispatcherStrategyMock.Object, healthStatusCallbackMock.Object, config, loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new ConsumeResultDispatcher<byte[], byte[]>(callbackMock.Object, null, healthStatusCallbackMock.Object, config, loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new ConsumeResultDispatcher<byte[], byte[]>(callbackMock.Object, dispatcherStrategyMock.Object, healthStatusCallbackMock.Object, null, loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new ConsumeResultDispatcher<byte[], byte[]>(callbackMock.Object, dispatcherStrategyMock.Object, healthStatusCallbackMock.Object, config, null));
        }

            [Test]
        public async Task TryEnqueueAsync_ChannelFull_ReturnFalse()
        {
            channelMock.Setup(c => c.TryWrite(It.IsAny<ConsumeResult<byte[], byte[]>>(), It.IsAny<byte>()))
                .Returns(false);

            var channel = channelMock.Object;
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(false);

            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            {
                Assert.That(await dispatcher.TryEnqueueAsync(fakeResult, cts.Token), Is.False);
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task TryEnqueueAsync_ChannelsNotFull_ReturnTrue(bool channelExists)
        {
            channelMock.Setup(c => c.TryWrite(It.IsAny<ConsumeResult<byte[], byte[]>>(), It.IsAny<byte>()))
                       .Returns(true);

            var channel = channelMock.Object;
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(channelExists);

            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            {
                Assert.That(await dispatcher.TryEnqueueAsync(fakeResult, cts.Token), Is.True);
            }
        }


        [Test]
        public async Task TryEnqueueAsync_TriggerCancellation_ExpectCleanUp()
        {
            channelMock.Setup(c => c.TryWrite(It.IsAny<ConsumeResult<byte[], byte[]>>(), It.IsAny<byte>()))
                       .Returns(true);

            var channel = channelMock.Object;
            dispatcherStrategyMock.Setup(d => d.CreateOrGet(It.IsAny<ConsumeResult<byte[], byte[]>>(), out channel))
                .Returns(true);

            using (var cts = new CancellationTokenSource())
            {
                Assert.That(await dispatcher.TryEnqueueAsync(fakeResult, cts.Token), Is.True);

                cts.Cancel();

                await Task.Delay(TimeSpan.FromSeconds(5));
            }

            dispatcherStrategyMock.Verify(ds => ds.Remove(It.IsAny<IConsumeResultChannel<byte[], byte[]>>()), Times.Once);
        }
    }
}
