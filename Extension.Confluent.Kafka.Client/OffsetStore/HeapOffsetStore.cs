using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Extension.Confluent.Kafka.Client.OffsetStore
{
    internal class HeapOffsetStore<TKey, TValue> : IOffsetStore<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> consumer;
        private readonly OffsetStoreConfig configuration;
        //Note: required to sync clear with other operations
        private readonly ReaderWriterLockSlim slimLock = new ReaderWriterLockSlim();

        private ConcurrentDictionary<TopicPartition, OffsetHeap> offsetHeapsByTopicPartition;
        private ConcurrentDictionary<TopicPartitionOffset, OffsetHeap.OffsetHeapItem> offsetHeapItemsByTopicPartitionOffset;

        public HeapOffsetStore(IConsumer<TKey, TValue> consumer, OffsetStoreConfig configuration)
        {
            this.consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
            this.configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            offsetHeapsByTopicPartition = new ConcurrentDictionary<TopicPartition, OffsetHeap>();
            offsetHeapItemsByTopicPartitionOffset = new ConcurrentDictionary<TopicPartitionOffset, OffsetHeap.OffsetHeapItem>();
        }

        public void Store(ReadOnlyMemory<ConsumeResult<TKey, TValue>> results)
        {
            slimLock.EnterReadLock();

            try
            {
                for (int i = 0; i < results.Length; i++)
                {
                    Store(results.Span[i]);
                }
            }
            finally
            {
                slimLock.ExitReadLock();
            }
        }

        private void Store(ConsumeResult<TKey, TValue> message)
        {
            if (!offsetHeapItemsByTopicPartitionOffset.ContainsKey(message.TopicPartitionOffset))
            {
                offsetHeapItemsByTopicPartitionOffset[message.TopicPartitionOffset] = new OffsetHeap.OffsetHeapItem(message.TopicPartition, message.Offset);
            }

            if (!offsetHeapsByTopicPartition.TryGetValue(message.TopicPartition, out var offsetHeap))
            {
                offsetHeap = new OffsetHeap(configuration.DefaultHeapSize);

                if (!offsetHeapsByTopicPartition.TryAdd(message.TopicPartition, offsetHeap))
                {
                    offsetHeap.Dispose();
                }
            }

            offsetHeap.Add(offsetHeapItemsByTopicPartitionOffset[message.TopicPartitionOffset]);
        }

        public void Complete(ReadOnlyMemory<ConsumeResult<TKey, TValue>> results)
        {
            slimLock.EnterReadLock();

            try
            {
                for (int i = 0; i < results.Length; i++)
                {
                    Complete(results.Span[i]);
                }
            }
            finally
            {
                slimLock.ExitReadLock();
            }
        }

        private void Complete(ConsumeResult<TKey, TValue> message)
        {
            //Note: we have to remove offsetHeapItem to prevent memory leak, offsetHeapItem will be marked for removal 
            if (offsetHeapItemsByTopicPartitionOffset.TryRemove(message.TopicPartitionOffset, out var offsetHeapItem))
            {
                //Note: TopicPartitionOffset are not processed in parallel we don't need to sync, since we don't receive twice. 
                //Only edge case can be consume group re-balancing, where TopicPartitionOffset is consumed again
                offsetHeapItem.Removeable = true;
            }
        }

        public IEnumerable<TopicPartitionOffset> FlushCommit()
        {
            var committableOffsets = new List<TopicPartitionOffset>();

            slimLock.EnterReadLock();

            try
            {
                foreach (var topicPartition in offsetHeapsByTopicPartition.Keys.ToArray())
                {
                    if (offsetHeapsByTopicPartition.TryGetValue(topicPartition, out var offsetHeap))
                    {
                        OffsetHeap.OffsetHeapItem? highestOffset = null;
                        while (offsetHeap.TryPeek(out var offsetItem))
                        {
                            if (!offsetItem.Removeable)
                                break;

                            if (offsetHeap.TryRemove(offsetItem))
                            {
                                highestOffset = offsetItem;
                            }
                        }

                        if (highestOffset != null)
                        {
                            // Note: The committed offset should always be the offset of the next message that your application will read. Thus, when calling commitSync(offsets) you should add one to the offset of the last message processed. 
                            // (see https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)
                            committableOffsets.Add(new TopicPartitionOffset(highestOffset.TopicPartition, highestOffset.Offset + 1));
                        }
                    }
                }
            }
            finally
            {
                slimLock.ExitReadLock();
            }

            if (committableOffsets.Count > 0)
            {
                consumer.Commit(committableOffsets);
            }

            return committableOffsets;
        }

        public void Clear(IEnumerable<TopicPartitionOffset>? obsoleteTopicPartitions = null)
        {
            slimLock.EnterWriteLock();
            try
            {

                //Note: offsetHeapItemsByTopicPartitionOffset doesn't need to be cleaned, will be removed automatically if Complete is called 
                if (obsoleteTopicPartitions == null)
                {
                    offsetHeapsByTopicPartition = new ConcurrentDictionary<TopicPartition, OffsetHeap>();
                }
                else
                {
                    foreach(var obsoleteTopicPartition in obsoleteTopicPartitions)
                    {
                        offsetHeapsByTopicPartition.TryRemove(obsoleteTopicPartition.TopicPartition, out _);
                    }
                }
            }
            finally
            {
                slimLock.ExitWriteLock();
            }
        }

        public void Dispose()
        {
            slimLock.Dispose();
        }

        private class OffsetHeap : IDisposable
        {
            private const int GrowthFactor = 2;

            private readonly object lockObject = new object();

            private OffsetHeapItem[] offsetItems;
            private int count;
            private int capacity;

            public OffsetHeap(int capacity)
            {
                if (capacity < 1) throw new ArgumentException($"{nameof(capacity)} has to be > 0");

                this.capacity = capacity;
                this.offsetItems = new OffsetHeapItem[capacity];
            }

            public void Add(OffsetHeapItem offset)
            {
                lock (lockObject)
                {
                    if (count == capacity)
                    {
                        SetCapacity(capacity * GrowthFactor);
                    }

                    offsetItems[count] = offset;

                    MoveOffsetUp(count);

                    count++;
                }
            }

            public bool TryPeek(out OffsetHeapItem item)
            {
                lock (lockObject)
                {
                    if (this.count == 0)
                    {
                        item = null!;
                        return false;
                    }

                    item = offsetItems[0];
                    return true;
                }
            }

            public bool TryRemove(OffsetHeapItem offsetHeapItem)
            {
                lock (lockObject)
                {
                    if (this.count == 0)
                    {
                        return false;
                    }

                    var v = offsetItems[0];

                    if (v.TopicPartition == offsetHeapItem.TopicPartition && v.Offset == offsetHeapItem.Offset)
                    {
                        count -= 1;
                        offsetItems[0] = offsetItems[count];
                        offsetItems[count] = default(OffsetHeapItem)!;

                        MoveOffsetDown(0);

                        return true;
                    }

                    return false;
                }
            }

            private void SetCapacity(int newCapacity)
            {
                newCapacity = Math.Max(newCapacity, count);
                if (capacity != newCapacity)
                {
                    capacity = newCapacity;
                    Array.Resize(ref offsetItems, capacity);
                }
            }

            private void MoveOffsetUp(int position)
            {
                var index = position;
                var item = offsetItems[index];

                int parent;
                while ((parent = (index - 1) >> 1) > -1 && item.Offset < offsetItems[parent].Offset)
                {
                    offsetItems[index] = offsetItems[parent];
                    index = parent;
                }

                offsetItems[index] = item;
            }

            private void MoveOffsetDown(int position)
            {
                var parent = position;
                var item = offsetItems[parent];

                int leftChild;
                while ((leftChild = (parent << 1) + 1) < count)
                {
                    int index = leftChild;
                    var rightChild = leftChild + 1;
                    if (rightChild < count)
                    {
                        //min heap
                        index = offsetItems[leftChild].Offset < offsetItems[rightChild].Offset ? leftChild : rightChild;
                    }

                    if (item.Offset < offsetItems[index].Offset)
                        break;

                    offsetItems[parent] = offsetItems[index];
                    parent = index;
                }

                offsetItems[parent] = item;
            }

            private void Clear()
            {
                lock (lockObject)
                {
                    this.count = 0;
                    Array.Clear(offsetItems, 0, offsetItems.Length);
                }
            }

            public void Dispose()
            {
                Clear();
            }

            internal class OffsetHeapItem
            {
                public TopicPartition TopicPartition { get; }

                public long Offset { get; }

                public bool Removeable { get; set; }

                public OffsetHeapItem(TopicPartition topicPartition, long offset)
                {
                    TopicPartition = topicPartition;
                    Offset = offset;
                    Removeable = false;
                }
            }
        }
    }
}
