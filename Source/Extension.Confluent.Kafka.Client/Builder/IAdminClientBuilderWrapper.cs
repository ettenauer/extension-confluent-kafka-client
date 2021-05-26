using Confluent.Kafka;

namespace Extension.Confluent.Kafka.Client.Builder
{
    public interface IAdminClientBuilderWrapper
    {
        IAdminClient Build();
    }
}
