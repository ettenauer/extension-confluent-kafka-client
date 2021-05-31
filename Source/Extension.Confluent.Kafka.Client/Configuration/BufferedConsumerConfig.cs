﻿using System.Collections.Generic;

namespace Extension.Confluent.Kafka.Client.Configuration
{
    public class BufferedConsumerConfig
    {
        public BufferSharding BufferSharding { get; init; }

        public short? BufferMaxTaskCount { get; init; }

        public short BufferSizePerChannel { get; init; }

        public int BufferCommitIntervalInMilliseconds { get; init; }

        public int BackpressurePartitionPauseInMilliseconds { get; init; }

        public int? PingIntervalInMilliseconds { get; init; }

        public int ConnectionTimeoutInMilliseconds { get; init; }

        public int CallbackResultCount { get; init; }

        public IEnumerable<BufferedTopicConfig> TopicConfigs { get; init; } = null!;
    }
}
