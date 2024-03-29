﻿using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy
{
    internal class TaskStrategy<TKey, TValue> : IDispatcherStrategy<TKey, TValue>
    {
        private readonly ReaderWriterLockSlim slimLock = new ReaderWriterLockSlim();

        private readonly ConcurrentDictionary<long, ConsumeResultChannel<TKey, TValue>> channels;
        private readonly Func<ConsumeResult<TKey, TValue>, long> getChannelIdFunc;
        private readonly short channelSize;
        private readonly byte priorityChannelCount;
        private readonly short maxTaskCount;

        public TaskStrategy(Func<ConsumeResult<TKey, TValue>, long> getChannelIdFunc, short channelSize, byte priorityChannelCount, short maxTaskCount)
        {
            if (channelSize < 1) throw new ArgumentException($"{nameof(channelSize)} has to be > 0");
            if (priorityChannelCount < 1) throw new ArgumentException($"{nameof(priorityChannelCount)} has to be > 0");
            if (maxTaskCount < 1) throw new ArgumentException($"{nameof(maxTaskCount)} has to be > 0");
            this.channelSize = channelSize;
            this.priorityChannelCount = priorityChannelCount;
            this.maxTaskCount = maxTaskCount;
            this.channels = new ConcurrentDictionary<long, ConsumeResultChannel<TKey, TValue>>();
            this.getChannelIdFunc = getChannelIdFunc ?? throw new ArgumentNullException(nameof(getChannelIdFunc));
        }

        public bool CreateOrGet(ConsumeResult<TKey, TValue> result, out IConsumeResultChannel<TKey, TValue> channel)
        {
            //IMPORTANT: the internal channel id calculate based modulo of channel count setting in order to control required resources
            var internalChannelId = getChannelIdFunc(result) % maxTaskCount;

            slimLock.EnterReadLock();

            try
            {
                if (!channels.ContainsKey(internalChannelId))
                {
                    var newChannel = new ConsumeResultChannel<TKey, TValue>(internalChannelId, channelSize, priorityChannelCount);
                    if (channels.TryAdd(internalChannelId, newChannel))
                    {
                        channel = newChannel;
                        return true;
                    }
                }

                channel = channels[internalChannelId];
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
