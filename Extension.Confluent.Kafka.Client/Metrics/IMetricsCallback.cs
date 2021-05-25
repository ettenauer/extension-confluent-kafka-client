using System;

namespace Extension.Confluent.Kafka.Client.Metrics
{
    public interface IMetricsCallback
    {
        void OnReceived(int messageCount, TimeSpan callbackDuration);
    }
}
