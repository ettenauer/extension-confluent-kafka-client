using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Consumer;
using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Tests.Consumer
{
    [TestFixture]
    public class ConsumeResultChannelTests
    {
        private ConsumeResult<byte[], byte[]> fakeResult;

        public void SetUp()
        {
            fakeResult = new ConsumeResult<byte[], byte[]>();
        }

        [Test]
        public void TryWrite_NotExistingPrio_ThrowException()
        {
            var channel = new ConsumeResultChannel<byte[], byte[]>(channelId: 1, channelSize: 2, priorityChannelCount: 1);

            //Note: 2 doesn't exists as prio
            Assert.Throws<ArgumentException>(() => channel.TryWrite(fakeResult, 1));
        }

        [Test]
        public void TryWrite_ChannelSizeReached_ReturnFalse()
        {
            var channel = new ConsumeResultChannel<byte[], byte[]>(channelId: 1, channelSize: 2, priorityChannelCount: 1);

            Assert.That(channel.TryWrite(fakeResult, 0), Is.True);
            Assert.That(channel.TryWrite(fakeResult, 0), Is.True);
            Assert.That(channel.TryWrite(fakeResult, 0), Is.False);
        }

        [Test]
        public void TryWrite_HighPrioFullLowPriorItem_ReturnTrue()
        {
            var channel = new ConsumeResultChannel<byte[], byte[]>(channelId: 1, channelSize: 2, priorityChannelCount: 2);

            Assert.That(channel.TryWrite(fakeResult, 0), Is.True);
            Assert.That(channel.TryWrite(fakeResult, 0), Is.True);
            Assert.That(channel.TryWrite(fakeResult, 0), Is.False);
            Assert.That(channel.TryWrite(fakeResult, 1), Is.True);
        }

        [Test]
        public void TryWrite_LowPrioFullHighPrioItem_ReturnTrue()
        {
            var channel = new ConsumeResultChannel<byte[], byte[]>(channelId: 1, channelSize: 2, priorityChannelCount: 2);

            Assert.That(channel.TryWrite(fakeResult, 1), Is.True);
            Assert.That(channel.TryWrite(fakeResult, 1), Is.True);
            Assert.That(channel.TryWrite(fakeResult, 1), Is.False);
            Assert.That(channel.TryWrite(fakeResult, 0), Is.True);
        }

        [Test]
        public void WriteFake_NewItem_ReadNull()
        {
            var channel = new ConsumeResultChannel<byte[], byte[]>(channelId: 1, channelSize: 2, priorityChannelCount: 2);

            channel.WriteFake();

            channel.TryRead(out var result);

            Assert.That(result, Is.Null);
        }

        [Test]
        public void TryRead_MixedPriorItems_ReadHighPrioFirst()
        {
            var channel = new ConsumeResultChannel<byte[], byte[]>(channelId: 1, channelSize: 2, priorityChannelCount: 2);

            var result1 = new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test", 1, 1) };
            Assert.That(channel.TryWrite(result1, 1), Is.True);
            var result2 = new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test", 2, 2) };
            Assert.That(channel.TryWrite(result2, 0), Is.True);

            Assert.That(channel.TryRead(out var readResult), Is.True);
            Assert.That(readResult, Is.EqualTo(result2));
            Assert.That(channel.TryRead(out readResult), Is.True);
            Assert.That(readResult, Is.EqualTo(result1));
        }

        [Test]
        public void TryRead_Empty_ReturnFalse()
        {
            var channel = new ConsumeResultChannel<byte[], byte[]>(channelId: 1, channelSize: 2, priorityChannelCount: 2);

            Assert.That(channel.TryRead(out _), Is.False);
        }

        [TestCase(0)]
        [TestCase(1)]
        public async Task WaitToReadAsync_NewItemAnyPrio_Unblock(byte priority)
        {
            var channel = new ConsumeResultChannel<byte[], byte[]>(channelId: 1, channelSize: 2, priorityChannelCount: 2);
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            {
                var waitTask = Task.Run(async () => await channel.WaitToReadAsync(cts.Token).ConfigureAwait(false));

                //if write doesn't unblock OperationCanceledException is thrown
                channel.TryWrite(fakeResult, priority);

                await waitTask.ConfigureAwait(false);
            }
        }

        [Test]
        public async Task WaitToReadAsync_Cancellation_Unblock()
        {
            var channel = new ConsumeResultChannel<byte[], byte[]>(channelId: 1, channelSize: 2, priorityChannelCount: 2);

            var stopwatch = Stopwatch.StartNew();
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                var token = cts.Token;
                try
                {
                    await channel.WaitToReadAsync(token).ConfigureAwait(false);

                    Assert.Fail("check cancellation code");
                }
                catch (OperationCanceledException e) when (e.CancellationToken == token)
                {
                    stopwatch.Stop();
                    if (stopwatch.Elapsed < TimeSpan.FromSeconds(5))
                        Assert.Fail($"Timeout to short {stopwatch.Elapsed}, check canncellation code");
                        
                    TestContext.Out.Write("Cancellation received yout to timeout");
                }
            }
        }
    }
 }
