using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy
{
    internal class PartitionStrategy<TKey, TValue> : IDispatcherStrategy<TKey, TValue>
    {
        private readonly ReaderWriterLockSlim slimLock = new ReaderWriterLockSlim();

        private readonly ConcurrentDictionary<long, ConsumeResultChannel<TKey, TValue>> channels;
        private readonly int channelSize;
        private readonly byte priorityChannelCount;

        public PartitionStrategy(int channelSize, byte priorityChannelCount)
        {
            if (channelSize < 1) throw new ArgumentException($"{nameof(channelSize)} has to be > 0");
            if (priorityChannelCount < 1) throw new ArgumentException($"{nameof(priorityChannelCount)} has to be > 0");
            this.channelSize = channelSize;
            this.priorityChannelCount = priorityChannelCount;
            this.channels = new ConcurrentDictionary<long, ConsumeResultChannel<TKey, TValue>>();
        }

        public bool CreateOrGet(ConsumeResult<TKey, TValue> result, out IConsumeResultChannel<TKey, TValue> workerChannel)
        {
            var channelId = result.TopicPartition.Partition.Value;

            slimLock.EnterReadLock();

            try
            {
                if (!channels.ContainsKey(channelId))
                {
                    var newChannel = new ConsumeResultChannel<TKey, TValue>(channelId, channelSize, priorityChannelCount);
                    if (channels.TryAdd(channelId, newChannel))
                    {
                        workerChannel = newChannel;
                        return true;
                    }
                }

                workerChannel = channels[channelId];
                return false;
            }
            finally
            {
                slimLock.ExitReadLock();
            }
        }

        public void Remove(IConsumeResultChannel<TKey, TValue> channel)
        {
            slimLock.EnterWriteLock();

            try
            {
                channels.TryRemove(channel.Id, out var _);
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
    }
}
