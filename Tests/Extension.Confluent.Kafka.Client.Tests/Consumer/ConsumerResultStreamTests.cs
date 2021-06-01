using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Configuration;
using Extension.Confluent.Kafka.Client.Consumer;
using Extension.Confluent.Kafka.Client.Extensions;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Extension.Confluent.Kafka.Client.Tests.Consumer
{
    [TestFixture]
    public class ConsumerResultStreamTests
    {
        private ConsumeResultStream<byte[], byte[]> stream;
        private Mock<IConsumer<byte[], byte[]>> consumerMock;
        private List<BufferedTopicConfig> topicConfig;
        private Mock<ILogger> loggerMock;

        [SetUp]
        public void SetUp()
        {
            consumerMock = new Mock<IConsumer<byte[], byte[]>>();
            topicConfig = new List<BufferedTopicConfig>
            {
                new BufferedTopicConfig
                {
                    TopicName = "Test",
                    Priority = 1
                }
            };
            loggerMock = new Mock<ILogger>();
            stream = new ConsumeResultStream<byte[], byte[]>(consumerMock.Object, topicConfig, loggerMock.Object);
        }

        [Test]
        public void Ctor_ArgumentValidation_ThrowException()
        {
            Assert.Throws<ArgumentNullException>(() => new ConsumeResultStream<byte[], byte[]>(null, topicConfig, loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new ConsumeResultStream<byte[], byte[]>(consumerMock.Object, null, loggerMock.Object));

            Assert.Throws<ArgumentNullException>(() => new ConsumeResultStream<byte[], byte[]>(consumerMock.Object, topicConfig, null));
        }

        [Test]
        public void Pause_TopicPartitionOffset_StopTopicPartition()
        {
            var result = new ConsumeResult<byte[], byte[]>
            {
                TopicPartitionOffset = new TopicPartitionOffset("Test", 1, 2)
            };

            stream.Assign(new List<TopicPartition>() { new TopicPartition("Test", 1) });

            Assert.That(stream.Pause(result), Is.EqualTo(result.TopicPartitionOffset));
            consumerMock.Verify(c => c.Pause(It.Is<IEnumerable<TopicPartition>>(_ => _.All(tp => tp.Topic == "Test" && tp.Partition == 1))), Times.Once);
        }

        [Test]
        public void Pause_TopicPartitionNotExists_StopTopicPartition()
        {
            var result = new ConsumeResult<byte[], byte[]>
            {
                TopicPartitionOffset = new TopicPartitionOffset("Test2", 1, 2)
            };

            //Note: assinged to Test:1 and not Test2:1
            stream.Assign(new List<TopicPartition>() { new TopicPartition("Test", 1) });

            Assert.That(stream.Pause(result), Is.Null);
            consumerMock.Verify(c => c.Pause(It.Is<IEnumerable<TopicPartition>>(_ => _.All(tp => tp.Topic == "Test2" && tp.Partition == 1))), Times.Never);
        }

        [Test]
        public void Resume_StoppedTopicParition_SeekAndResumeTopicPartition()
        {
            var result = new ConsumeResult<byte[], byte[]>
            {
                TopicPartitionOffset = new TopicPartitionOffset("Test", 1, 2)
            };

            stream.Assign(new List<TopicPartition>() { new TopicPartition("Test", 1) });

            Assert.That(stream.Pause(result), Is.Not.Null);

            stream.Resume(new List<TopicPartitionOffset> { new TopicPartitionOffset("Test", 1, 2) });

            consumerMock.Verify(_ => _.Seek(It.Is<TopicPartitionOffset>(tpo => tpo.Topic == "Test" && tpo.Partition == 1 && tpo.Offset == 2)), Times.Once);
            consumerMock.Verify(_ => _.Resume(It.Is<IEnumerable<TopicPartition>>(l => l.All(tpo => tpo.Topic == "Test" && tpo.Partition == 1))), Times.Once);
        }

        [Test]
        public void Consume_QueuedMessages_ExpectPriorityHeader()
        {
            stream.Assign(new List<TopicPartition>() { new TopicPartition("Test", 1) });

            consumerMock.Setup(_ => _.Consume(It.IsAny<CancellationToken>()))
                .Returns(new ConsumeResult<byte[], byte[]>
                {
                    TopicPartitionOffset = new TopicPartitionOffset("Test", 1, 2)
                });

            foreach (var result in stream.Consume(TimeSpan.FromSeconds(1)))
            {
                Assert.That(result.Message.Headers.GetTopicPriority(), Is.EqualTo(1));
            }
        }

        [Test]
        public void Consume_TimeoutReached_ExpectEmpty()
        {
            stream.Assign(new List<TopicPartition>() { new TopicPartition("Test", 1) });

            consumerMock.Setup(_ => _.Consume(It.IsAny<CancellationToken>()))
                .Returns((ConsumeResult<byte[], byte[]>) null);

            Assert.That(stream.Consume(TimeSpan.FromSeconds(1)), Is.Empty);   
        }


        [Test]
        public void Subscribe_AssignedTopics_CalledWithTopics()
        {
            stream.Assign(new List<TopicPartition>() { new TopicPartition("Test", 1) });

            stream.Subscribe();

            consumerMock.Verify(_ => _.Subscribe(It.Is<IEnumerable<string>>(_ => _.All(t =>  t == "Test"))), Times.Once);
        }
    }
}
