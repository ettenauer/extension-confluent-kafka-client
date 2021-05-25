using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Configuration;
using Extension.Confluent.Kafka.Client.Consumer;
using Extension.Confluent.Kafka.Client.Consumer.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Local.Runner
{
    class Program
    {
        static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection
                .AddLogging(configure => configure.AddConsole())
                .AddTransient<Program>();

            serviceCollection
                .AddLogging(configure => configure.AddConsole())
                .AddTransient<Callback>();

            var serviceProvider = serviceCollection.BuildServiceProvider();
            var logger = serviceProvider.GetService<ILoggerFactory>()
                                        .CreateLogger<Program>();
            logger.LogInformation("setup consumer");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var bufferedConsumerConfig = new BufferedConsumerConfig
            {
                BufferSharding = BufferSharding.Task,
                BufferMaxTaskCount = 4,
                BufferCommitIntervalInMilliseconds = 1000,
                CallbackResultCount = 10,
                BufferSizePerChannel = 100,
                ConnectionTimeoutInMilliseconds = 5000,
                BackpressurePartitionPauseInMilliseconds = 1000,
                TopicConfigs = new List<BufferedTopicConfig>
                {
                    new BufferedTopicConfig
                    {
                        TopicName = "topic.test",
                    }
                }
            };

            var internalConsumerBuilder = new ConsumerBuilder<byte[], byte[]>(consumerConfig);
            var adminClientBuilder = new AdminClientBuilder(consumerConfig);
            var bufferedConsumer = new BufferedConsumerBuilder<byte[], byte[]>(bufferedConsumerConfig)
                .SetConsumerBuilder(internalConsumerBuilder)
                .SetAdminBuilder(adminClientBuilder)
                .SetChannelIdFunc((r) => r.Partition)
                .SetCallback(new Callback(logger))
                .SetLogger(logger)
                .Build();

            logger.LogInformation("Start BufferedConsumer");

            bufferedConsumer.Subscribe();

            Console.ReadLine();

            logger.LogInformation("Stop BufferedConsumer");

            bufferedConsumer.Dispose();

            Console.ReadLine();
        }

        private class Callback : IConsumeResultCallback<byte[], byte[]>
        {
            private readonly ILogger logger;
            public Callback(ILogger logger)
            {
                this.logger = logger;
            }

            public Task OnReceivedAsync(ReadOnlyMemory<ConsumeResult<byte[], byte[]>> results, CancellationToken cancellationToken)
            {
                logger.LogDebug("received results [0]", results.Length);

                return Task.CompletedTask;
            }
        }
    }
}
