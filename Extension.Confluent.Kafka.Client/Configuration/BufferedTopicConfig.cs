namespace Extension.Confluent.Kafka.Client.Configuration
{
    public class BufferedTopicConfig
    {
        public string TopicName { get; set; } = null!;

        public byte Priority { get; set; }
    }
}
