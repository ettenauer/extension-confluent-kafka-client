using System;

namespace Extension.Confluent.Kafka.Client.Metrics
{
    public interface IMetricsCallback
    {
        void Record(int messageCount, TimeSpan callbackDuration);
    }
}
