using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Consumer
{
    public interface IConsumeResultChannel<TKey, TValue>
    {
        long Id { get; }
        bool TryWrite(ConsumeResult<TKey, TValue>? item, byte priority);
        void WriteFake();
        Task<bool> WaitToReadAsync(CancellationToken cancellationToken);
        bool TryRead(out ConsumeResult<TKey, TValue>? item);
    }
}
