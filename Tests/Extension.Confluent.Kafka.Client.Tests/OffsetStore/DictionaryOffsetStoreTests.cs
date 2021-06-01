using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.OffsetStore;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Extension.Confluent.Kafka.Client.Tests.OffsetStore
{
    [TestFixture]
    public class DictionaryOffsetStoreTests
    {
        private DictionaryOffsetStore<byte[], byte[]> offsetStore;
        private Mock<IConsumer<byte[], byte[]>> consumerMock;

        [SetUp]
        public void SetUp()
        {
            consumerMock = new Mock<IConsumer<byte[], byte[]>>();
            offsetStore = new DictionaryOffsetStore<byte[], byte[]>(consumerMock.Object);
        }

        [Test]
        public void Ctor_ArgumentValidation_ThrowException()
        {
            Assert.Throws<ArgumentNullException>(() => new DictionaryOffsetStore<byte[], byte[]>(null));
        }

        [Test]
        public void FlushCommit_TrackOffsets_ExpectCompletedOffsets()
        {
            var enqueuedResults = new ConsumeResult<byte[], byte[]>[]
            {
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test1", 1, 1)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test1", 1, 2)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test2", 2, 3)}
            };

            offsetStore.Store(enqueuedResults);

            var completedResults = new ConsumeResult<byte[], byte[]>[]
            {
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test1", 1, 1)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test2", 2, 3)}
            };

            offsetStore.Complete(completedResults);

            offsetStore.FlushCommit();

            consumerMock.Verify(_ => _.Commit(It.Is<IEnumerable<TopicPartitionOffset>>(
                tp => tp.Count() == 2 &&
                tp.Count(tpo => tpo.Topic == "test1" && tpo.Partition == 1 && tpo.Offset == 1 + 1) == 1 &&
                tp.Count(tpo => tpo.Topic == "test2" && tpo.Partition == 2 && tpo.Offset == 3 + 1) == 1)),
                Times.Once);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void Clear_TrackOffsets_RemovedOffsets(bool all)
        {
            var enqueuedResults = new ConsumeResult<byte[], byte[]>[]
            {
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test1", 1, 1)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test1", 1, 2)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test2", 2, 3)}
            };

            offsetStore.Store(enqueuedResults);

            var completedResults = new ConsumeResult<byte[], byte[]>[]
            {
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test1", 1, 1)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test2", 2, 3)}
            };

            offsetStore.Complete(completedResults);

            if (all)
            {
                offsetStore.Clear();
            }
            else
            {
                offsetStore.Clear(new[] { new TopicPartitionOffset("test1", 1, 1) });
            }

            offsetStore.FlushCommit();

            if (all)
            {
                consumerMock.Verify(_ => _.Commit(It.IsAny<IEnumerable<TopicPartitionOffset>>()), Times.Never);
            }
            else
            {
                consumerMock.Verify(_ => _.Commit(It.Is<IEnumerable<TopicPartitionOffset>>(
                                    tp => tp.Count() == 1 &&
                                    tp.Count(tpo => tpo.Topic == "test2" && tpo.Partition == 2 && tpo.Offset == 3 + 1) == 1)),
                                    Times.Once);
            }
        }
    }
}
