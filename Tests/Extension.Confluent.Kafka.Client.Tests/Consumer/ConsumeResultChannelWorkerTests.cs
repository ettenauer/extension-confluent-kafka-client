using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Consumer;
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
    public class ConsumeResultChannelWorkerTests
    {
        private ConsumeResultChannelWorker<byte[], byte[]> channelWorker;
        private Mock<IConsumeResultChannel<byte[], byte[]>> channelMock;
        private Mock<IConsumeResultCallback<byte[], byte[]>> callbackMock;
        private ConsumeResultChannelWorkerConfig config;
        private Mock<ILogger> loggerMock;
        private Mock<IHealthStatusCallback> healthStatusCallbackMock;
        private IConsumeResultChannel<byte[], byte[]> removedChannel;

        [SetUp]
        public void SetUp()
        {
            callbackMock = new Mock<IConsumeResultCallback<byte[], byte[]>>();
            callbackMock.Setup(cb => cb.OnReceivedAsync(It.IsAny<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            config = new ConsumeResultChannelWorkerConfig { CallbackResultCount = 2 };
            channelMock = new Mock<IConsumeResultChannel<byte[], byte[]>>();
            loggerMock = new Mock<ILogger>();
            healthStatusCallbackMock = new Mock<IHealthStatusCallback>();
            removedChannel = null;
            channelWorker = new ConsumeResultChannelWorker<byte[], byte[]>(
                channelMock.Object,
                callbackMock.Object,
                healthStatusCallbackMock.Object,
                (c) => { removedChannel = c; },
                config,
                loggerMock.Object            
                );
        }

        [Test]
        public void Ctor_ArgumentValidation_ThrowException()
        {
            Assert.Throws<ArgumentNullException>(() => new ConsumeResultChannelWorker<byte[], byte[]>(null, callbackMock.Object, healthStatusCallbackMock.Object, (c) => { removedChannel = c; }, config, loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new ConsumeResultChannelWorker<byte[], byte[]>(channelMock.Object, null, healthStatusCallbackMock.Object, (c) => { removedChannel = c; }, config, loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new ConsumeResultChannelWorker<byte[], byte[]>(channelMock.Object, callbackMock.Object, healthStatusCallbackMock.Object, null, config, loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new ConsumeResultChannelWorker<byte[], byte[]>(channelMock.Object, callbackMock.Object, healthStatusCallbackMock.Object, (c) => { removedChannel = c; }, null, loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new ConsumeResultChannelWorker<byte[], byte[]>(channelMock.Object, callbackMock.Object, healthStatusCallbackMock.Object, (c) => { removedChannel = c; }, config, null));
        }

        [Test]
        public async Task CreateRunTask_OneResult_OnReceivedAsync()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            {
                var fakeResult = new ConsumeResult<byte[], byte[]>();

                channelMock.Setup(c => c.WaitToReadAsync(It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(true));

                channelMock.SetupSequence(c => c.TryRead(out fakeResult))
                    .Returns(true)
                    .Returns(false);

                var workerTask = channelWorker.CreateRunTask(cts.Token);

                workerTask.Start();

                await Task.Delay(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                cts.Cancel();

                Assert.That(removedChannel, Is.Null);
                callbackMock.Verify(cb => cb.OnReceivedAsync(It.Is<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(cb => cb.Length == 1), It.IsAny<CancellationToken>()), Times.Exactly(1));
                cts.Cancel();
            }   
        }

        [Test]
        public async Task CreateRunTask_MoreThanMaxCallback_TwiceOnReceivedAsync()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            {
                var fakeResult = new ConsumeResult<byte[], byte[]>();

                channelMock.Setup(c => c.WaitToReadAsync(It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(true));

                channelMock.SetupSequence(c => c.TryRead(out fakeResult))
                    .Returns(true)
                    .Returns(true)
                    .Returns(true);

                var workerTask = channelWorker.CreateRunTask(cts.Token);

                workerTask.Start();

                await Task.Delay(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                Assert.That(removedChannel, Is.Null);
                callbackMock.Verify(cb => cb.OnReceivedAsync(It.Is<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(cb => cb.Length == 2), It.IsAny<CancellationToken>()), Times.Exactly(1));
                callbackMock.Verify(cb => cb.OnReceivedAsync(It.Is<ReadOnlyMemory<ConsumeResult<byte[], byte[]>>>(cb => cb.Length == 1), It.IsAny<CancellationToken>()), Times.Exactly(1));
                cts.Cancel();
            }
        }

        [Test]
        public async Task CreateRunTask_Cancellation_VerifyCleanUp()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            {
                var fakeResult = new ConsumeResult<byte[], byte[]>();

                channelMock.Setup(c => c.WaitToReadAsync(It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(true));

                channelMock.SetupSequence(c => c.TryRead(out fakeResult))
                    .Returns(true)
                    .Returns(true);

                var workerTask = channelWorker.CreateRunTask(cts.Token);

                workerTask.Start();

                await Task.Delay(TimeSpan.FromSeconds(3)).ConfigureAwait(false);

                Assert.That(removedChannel, Is.Null);

                cts.Cancel();

                await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);

                Assert.That(removedChannel, Is.Not.Null);    
            }
        }
    }
}
