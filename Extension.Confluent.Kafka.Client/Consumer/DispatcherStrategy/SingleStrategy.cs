using Confluent.Kafka;
using System;
using System.Collections.Concurrent;

namespace Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy
{
    internal class SingleStrategy<TKey, TValue> : IDispatcherStrategy<TKey, TValue>
    {
        private const long ChannelId = 1;

        private readonly ConcurrentDictionary<long, ConsumeResultChannel<TKey, TValue>> channels;
        private readonly int channelSize;
        private readonly byte priorityChannelCount;

        public SingleStrategy(int channelSize, byte priorityChannelCount)
        {
            if (channelSize < 1) throw new ArgumentException($"{nameof(channelSize)} has to be > 0");
            if (priorityChannelCount < 1) throw new ArgumentException($"{nameof(priorityChannelCount)} has to be > 0");
            this.channelSize = channelSize;
            this.priorityChannelCount = priorityChannelCount;
            this.channels = new ConcurrentDictionary<long, ConsumeResultChannel<TKey, TValue>>();
        }

        public bool CreateOrGet(ConsumeResult<TKey, TValue> message, out ConsumeResultChannel<TKey, TValue> workerChannel)
        {
            if (!channels.ContainsKey(ChannelId))
            {
                var newChannel = new ConsumeResultChannel<TKey, TValue>(ChannelId, channelSize, priorityChannelCount);
                if (channels.TryAdd(ChannelId, newChannel))
                {
                    workerChannel = newChannel;
                    return true;
                }
            }

            workerChannel = channels[ChannelId];
            return false;
        }

        public void Remove(ConsumeResultChannel<TKey, TValue> workerChannel)
        {
            channels.TryRemove(workerChannel.Id, out var _);
        }
    }
}
