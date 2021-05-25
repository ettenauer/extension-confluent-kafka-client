using System.Collections.Generic;

namespace Extension.Confluent.Kafka.Client.Configuration
{
    public class BufferedConsumerConfig
    {
        public BufferSharding BufferSharding { get; set; }

        public int? BufferMaxTaskCount { get; set; }

        public int BufferSizePerChannel { get; set; }

        public int BufferCommitIntervalInMilliseconds { get; set; }

        public int BackpressurePartitionPauseInMilliseconds { get; set; }

        public int? PingIntervalInMilliseconds { get; set; }

        public int ConnectionTimeoutInMilliseconds { get; set; }

        public int CallbackResultCount { get; set; }

        public IEnumerable<BufferedTopicConfig> TopicConfigs { get; set; } = null!;
    }
}
