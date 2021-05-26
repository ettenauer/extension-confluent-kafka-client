using Confluent.Kafka;
using System;
using System.Collections.Concurrent;

namespace Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy
{
    internal class TaskStrategy<TKey, TValue> : IDispatcherStrategy<TKey, TValue>
    {
        private readonly ConcurrentDictionary<long, ConsumeResultChannel<TKey, TValue>> channels;
        private readonly Func<ConsumeResult<TKey, TValue>, long> getChannelIdFunc;
        private readonly int channelSize;
        private readonly byte priorityChannelCount;
        private readonly int maxTaskCount;

        public TaskStrategy(Func<ConsumeResult<TKey, TValue>, long> getChannelIdFunc, int channelSize, byte priorityChannelCount, int maxTaskCount)
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

        public void Remove(IConsumeResultChannel<TKey, TValue> channel)
        {
            channels.TryRemove(channel.Id, out var _);
        }
    }
}
