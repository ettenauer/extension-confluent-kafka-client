using Confluent.Kafka;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Consumer
{
    internal class ConsumeResultChannel<TKey, TValue> : IConsumeResultChannel<TKey, TValue>
    {
        private readonly Channel<ConsumeResult<TKey, TValue>>[] channels;

        public ConsumeResultChannel(long channelId, int channelSize, byte priorityChannelCount)
        {
            Id = channelId;
            channels = new Channel<ConsumeResult<TKey, TValue>>[priorityChannelCount];

            for (int i = 0; i < channels.Length; i++)
            {
                channels[i] = Channel.CreateBounded<ConsumeResult<TKey, TValue>>(
                new BoundedChannelOptions(channelSize)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleWriter = true,
                    SingleReader = true
                });
            }
        }

        public long Id { get; }

        public bool TryWrite(ConsumeResult<TKey, TValue>? item, byte priority)
        {
            //note: channels index reflex priority 
            if (priority > channels.Length - 1) throw new ArgumentException($"priority {priority} is not supported");

            return channels[priority].Writer.TryWrite(item!);
        }

        public void WriteFake()
        {
            for (int i = 0; i < channels.Length; i++)
            {
                _ = channels[i].Writer.TryWrite(null!);
            }
        }

        public async Task<bool> WaitToReadAsync(CancellationToken cancellationToken)
        {
            return await await Task.WhenAny(channels.Select(_ => _.Reader.WaitToReadAsync(cancellationToken).AsTask()));
        }

        public bool TryRead(out ConsumeResult<TKey, TValue>? item)
        {
            item = null;

            for(int i = 0; i < channels.Length; i++)
            {
                if (channels[i].Reader.TryRead(out item))
                    return true;
            }

            return false;
        }
    }
}
