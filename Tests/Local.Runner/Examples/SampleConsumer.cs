using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Configuration;
using Extension.Confluent.Kafka.Client.Consumer;
using Extension.Confluent.Kafka.Client.Consumer.Builder;
using Extension.Confluent.Kafka.Client.Health;
using Extension.Confluent.Kafka.Client.Metrics;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Local.Runner.Examples
{
    internal class SampleConsumer :
        IHealthStatusCallback,
        IMetricsCallback, 
        IConsumeResultCallback<byte[], byte[]>,
        IDisposable
    {

        private const string Topic = "testTopic";

        private readonly ILogger logger;
        private readonly IBufferedConsumer<byte[], byte[]> consumer;

        public SampleConsumer(ILogger logger)
        {
            this.logger = logger;

            //Note: confluent configuration see https://github.com/confluentinc/confluent-kafka-dotnet
            var confluentConfig = new ConsumerConfig
            {
                GroupId = "test-group",
                ClientId = Environment.MachineName,
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            //Note: actual configuration for buffered consumer
            var config = new BufferedConsumerConfig
            {
                BufferSharding = BufferSharding.Task,
                BufferMaxTaskCount = 5,
                TopicConfigs = new[]
                {
                    new BufferedTopicConfig
                    {
                        TopicName = Topic
                    }
                }
            };

            consumer = new BufferedConsumerBuilder<byte[], byte[]>(config)
                .SetConsumerBuilder(new ConsumerBuilder<byte[], byte[]>(confluentConfig))
                .SetAdminBuilder(new AdminClientBuilder(confluentConfig))
                .SetCallback(this)
                .SetHealthStatusCallback(this)
                .SetMetricsCallback(this)
                .SetChannelIdFunc((p) => p.Partition)
                .SetLogger(logger)
                .Build();
        }

        public void Subscribe()
        {
            consumer.Subscribe();
        }

        public void OnMessageLoopPing()
        {
            logger.LogInformation("Message Loop Ping");
        }

        public void OnUnhealthyConnection(Exception exception)
        {
            logger.LogError(exception, "Connection Error:");
        }

        public void OnHealthyConnection()
        {
            logger.LogInformation("Connection healthy");
        }

        public void Record(int messageCount, TimeSpan callbackDuration)
        {
            logger.LogInformation("Received {0} messages", messageCount);
        }

        public Task OnReceivedAsync(ReadOnlyMemory<ConsumeResult<byte[], byte[]>> results, CancellationToken cancellationToken)
        {
            return Task.Delay(100, cancellationToken);
        }

        public void Dispose()
        {
            consumer.Dispose();
        }
    }
}
