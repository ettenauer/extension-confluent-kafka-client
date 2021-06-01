namespace Extension.Confluent.Kafka.Client.Configuration
{
    public class BufferedTopicConfig
    {
        /// <summary>
        /// Kafka Topic Name
        /// </summary>
        public string TopicName { get; init; } = null!;

        /// <summary>
        /// Topic Priority for consumption. Lower value means higher priority.
        /// </summary>
        public byte Priority { get; init; } = 0;
    }
}
