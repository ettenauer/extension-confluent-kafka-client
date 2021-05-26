using Confluent.Kafka;

namespace Extension.Confluent.Kafka.Client.Consumer.DispatcherStrategy
{
    public interface IDispatcherStrategy<TKey, TValue>
    {
        bool CreateOrGet(ConsumeResult<TKey, TValue> message, out IConsumeResultChannel<TKey, TValue> channel);
        void Remove(IConsumeResultChannel<TKey, TValue> channel);
    }
}
