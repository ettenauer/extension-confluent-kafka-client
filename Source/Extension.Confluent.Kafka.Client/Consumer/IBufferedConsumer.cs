using System;

namespace Extension.Confluent.Kafka.Client.Consumer
{
    public interface IBufferedConsumer<TKey, TValue> : IDisposable
    {
        void Subscribe();
        void Unsubscribe();
    }
}
