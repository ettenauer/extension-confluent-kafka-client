using System;

namespace Extension.Confluent.Kafka.Client.Health
{
    public interface IHealthStatusCallback
    {
        public void OnMessageLoopPing();

        public void OnUnhealthyConnection(Exception exception);

        public void OnHealthyConnection();
    }
}
