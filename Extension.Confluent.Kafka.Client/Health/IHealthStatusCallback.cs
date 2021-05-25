using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Health
{
    public interface IHealthStatusCallback
    {
        public void OnMessageLoopPing();

        public void OnUnhealthyConnection(Exception exception);

        public void OnHealthyConnection();
    }
}
