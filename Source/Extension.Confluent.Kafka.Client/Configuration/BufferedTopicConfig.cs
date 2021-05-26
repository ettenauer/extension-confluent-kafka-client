namespace Extension.Confluent.Kafka.Client.Configuration
{
    public class BufferedTopicConfig
    {
        public string TopicName { get; init; } = null!;

        public byte Priority { get; init; }
    }
}
