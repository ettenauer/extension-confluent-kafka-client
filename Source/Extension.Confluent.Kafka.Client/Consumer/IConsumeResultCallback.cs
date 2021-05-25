using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Consumer
{
    public interface IConsumeResultCallback<TKey, TValue>
    {
        public Task OnReceivedAsync(ReadOnlyMemory<ConsumeResult<TKey, TValue>> results, CancellationToken cancellationToken);
    }
}
