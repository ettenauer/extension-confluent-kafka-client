using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy;
using NUnit.Framework;
using System;

namespace Extension.Confluent.Kafka.Client.Tests.Consumer.DispatcherStrategy
{
    [TestFixture]
    public class TaskStrategyTests
    {
        private TaskStrategy<byte[], byte[]> strategy;
        private readonly short maxTaskCount = 5;

        [SetUp]
        public void SetUp()
        {
            strategy = new TaskStrategy<byte[], byte[]>((x) => x.Offset, 2, 1, maxTaskCount);
        }

        [Test]
        public void Ctor_ArgumentValidation_ThrowException()
        {
            //too small channel size
            Assert.Throws<ArgumentException>(() => new TaskStrategy<byte[], byte[]>((x) => x.Offset, 0, 1, 1));

            //invalid priority channel count
            Assert.Throws<ArgumentException>(() => new TaskStrategy<byte[], byte[]>((x) => x.Offset, 1, 0, 1));

            //invalid task count
            Assert.Throws<ArgumentException>(() => new TaskStrategy<byte[], byte[]>((x) => x.Offset, 1, 1, 0));
        }

        [Test]
        public void CreateOrGet_DifferentOffset_True()
        {
            var result1 = new ConsumeResult<byte[], byte[]>()
            {
                TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition("Test", 1), new Offset(1))
            };

            Assert.That(strategy.CreateOrGet(result1, out var channel), Is.True);
            Assert.That(channel.Id, Is.EqualTo(1));

            var result2 = new ConsumeResult<byte[], byte[]>()
            {
                TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition("Test", 2), new Offset(7))
            };

            Assert.That(strategy.CreateOrGet(result2, out channel), Is.True);
            Assert.That(channel.Id, Is.EqualTo(2)); //Note: 2 % maxTaskCount
        }

        [Test]
        public void CreateOrGet_SameTopicPartition_False()
        {
            var result1 = new ConsumeResult<byte[], byte[]>()
            {
                TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition("Test", 1), new Offset(3))
            };

            Assert.That(strategy.CreateOrGet(result1, out var channel), Is.True);
            Assert.That(channel.Id, Is.EqualTo(3));

            var result2 = new ConsumeResult<byte[], byte[]>()
            {
                TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition("Test", 2), new Offset(3))
            };

            Assert.That(strategy.CreateOrGet(result2, out channel), Is.False);
            Assert.That(channel.Id, Is.EqualTo(3));
        }

        [Test]
        public void Remove_ChannelExists_ExpectNewChannel()
        {
            var result1 = new ConsumeResult<byte[], byte[]>()
            {
                TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition("Test", 1), new Offset(1))
            };

            Assert.That(strategy.CreateOrGet(result1, out var channel), Is.True);
            Assert.That(strategy.CreateOrGet(result1, out channel), Is.False);

            strategy.Remove(channel);

            Assert.That(strategy.CreateOrGet(result1, out _), Is.True);
        }
    }
}
