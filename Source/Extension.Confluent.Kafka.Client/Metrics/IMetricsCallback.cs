using System;

namespace Extension.Confluent.Kafka.Client.Metrics
{
    public interface IMetricsCallback
    {
        void RecordSuccess(int messageCount, TimeSpan callbackDuration);

        void RecordFailure(int messageCount, TimeSpan callbackDuration);
    }
}
