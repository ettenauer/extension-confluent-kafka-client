using Confluent.Kafka;

namespace Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy
{
    internal interface IDispatcherStrategy<TKey, TValue>
    {
        bool CreateOrGet(ConsumeResult<TKey, TValue> message, out ConsumeResultChannel<TKey, TValue> channel);
        void Remove(ConsumeResultChannel<TKey, TValue> channel);
    }
}
