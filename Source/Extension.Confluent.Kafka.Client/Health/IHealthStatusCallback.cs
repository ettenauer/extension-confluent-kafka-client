using Confluent.Kafka;
using System;

namespace Extension.Confluent.Kafka.Client.Health
{
    public interface IHealthStatusCallback
    {
        public void OnMessageLoopPing();

        public void OnMessageLoopCancelled(string reason);

        public void OnWorkerTaskCancelled(long channelId, string reason);

        public void OnUnhealthyConnection(Exception exception);

        public void OnHealthyConnection();
    }
}
