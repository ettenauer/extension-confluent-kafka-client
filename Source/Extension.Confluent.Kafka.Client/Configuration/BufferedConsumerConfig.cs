using System.Collections.Generic;

namespace Extension.Confluent.Kafka.Client.Configuration
{
    public class BufferedConsumerConfig
    {
        /// <summary>
        /// BufferSharding setting controls how consumed messages are buffered and dispatched to tasks which triggeres defined callback.
        /// BufferSharding.Single -> A single task which consumes queued messages
        /// BufferShardig.Partition -> A task by partition consumes queued messages
        /// BufferSharding.Task -> X tasks (defined by BufferMaxTaskCount) consumes queued messages
        /// </summary>
        public BufferSharding BufferSharding { get; init; } = BufferSharding.Parition;

        /// <summary>
        /// Max number of tasks which are used in scope of BufferSharding.Task.
        /// </summary>
        public short? BufferMaxTaskCount { get; init; } = 1;

        /// <summary>
        /// The max size of queued messages per channel per task. If a channel is full then the consumpation for TopicPartition is stopped.
        /// </summary>
        public short BufferSizePerChannel { get; init; } = 100;

        /// <summary>
        /// Offsets are committed in the defined interval.
        /// </summary>
        public int BufferCommitIntervalInMilliseconds { get; init; } = 1000;

        /// <summary>
        /// Used to control backpressure in case channel is full since BufferSizePerChannel is reached.
        /// </summary>
        public int BackpressurePartitionPauseInMilliseconds { get; init; } = 1000;

        /// <summary>
        /// BufferedConsumer signals liveness by the defined interval. 
        /// </summary>
        public int? PingIntervalInMilliseconds { get; init; } = 5000;

        /// <summary>
        /// Timeout for BrokerConnection check.
        /// </summary>
        public int ConnectionTimeoutInMilliseconds { get; init; } = 8000;

        /// <summary>
        /// Size for the callback batches.
        /// </summary>
        public int CallbackResultCount { get; init; } = 10;

        /// <summary>
        /// Topics used for buffered consumption.
        /// </summary>
        public IEnumerable<BufferedTopicConfig> TopicConfigs { get; init; } = null!;
    }
}
