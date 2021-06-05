using Confluent.Kafka;
using System;

namespace Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy
{
    public interface IDispatcherStrategy<TKey, TValue> : IDisposable
    {
        bool CreateOrGet(ConsumeResult<TKey, TValue> message, out IConsumeResultChannel<TKey, TValue> channel);
        void Remove(IConsumeResultChannel<TKey, TValue> channel);
    }
}
