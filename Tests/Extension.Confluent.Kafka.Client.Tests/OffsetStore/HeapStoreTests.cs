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
    public class HeapStoreTests
    {
        private HeapOffsetStore<byte[], byte[]> offsetStore;
        private Mock<IConsumer<byte[], byte[]>> consumerMock;
        private OffsetStoreConfig config;

        [SetUp]
        public void SetUp()
        {
            config = new OffsetStoreConfig { DefaultHeapSize = 5 };
            consumerMock = new Mock<IConsumer<byte[], byte[]>>();
            offsetStore = new HeapOffsetStore<byte[], byte[]>(consumerMock.Object, config);
        }

        [Test]
        public void Ctor_ArgumentValidation_ThrowException()
        {
            Assert.Throws<ArgumentNullException>(() => new HeapOffsetStore<byte[], byte[]>(null, config));
            Assert.Throws<ArgumentNullException>(() => new HeapOffsetStore<byte[], byte[]>(consumerMock.Object, null));
        }

        [Test]
        public void FlushCommit_TrackOffsets_ExpectCompletedOffsets()
        {
            var enqueuedResults = new ConsumeResult<byte[], byte[]>[]
            {
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test1", 1, 1)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test1", 1, 2)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test2", 2, 3)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test2", 2, 4)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test2", 3, 5)}
            };

            offsetStore.Store(enqueuedResults);

            var completedResults = new ConsumeResult<byte[], byte[]>[]
            {
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test1", 1, 1)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test1", 1, 2)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test2", 2, 4)},
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test2", 3, 5)}
            };

            offsetStore.Complete(completedResults);

            offsetStore.FlushCommit();

            //Note: test2:2:4 cannot be committed since test2:2:3 is not completed yet
            consumerMock.Verify(_ => _.Commit(It.Is<IEnumerable<TopicPartitionOffset>>(
                tp => tp.Count() == 2 &&
                tp.Count(tpo => tpo.Topic == "test1" && tpo.Partition == 1 && tpo.Offset == 2 + 1) == 1 &&
                tp.Count(tpo => tpo.Topic == "test2" && tpo.Partition == 3 && tpo.Offset == 5 + 1) == 1)),
                Times.Once);

            completedResults = new ConsumeResult<byte[], byte[]>[]
            {
                new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset("test2", 2, 3)}
            };

            offsetStore.Complete(completedResults);

            offsetStore.FlushCommit();

            //Note: test2:2:3 is completed so test2:2:4 can be committed
            consumerMock.Verify(_ => _.Commit(It.Is<IEnumerable<TopicPartitionOffset>>(
            tp => tp.Count() == 1 &&
            tp.Count(tpo => tpo.Topic == "test2" && tpo.Partition == 2 && tpo.Offset == 4 + 1) == 1)),
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
