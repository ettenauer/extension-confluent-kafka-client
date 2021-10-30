using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Extension.Confluent.Kafka.Client.Configuration;
using Extension.Confluent.Kafka.Client.Consumer;
using Extension.Confluent.Kafka.Client.Consumer.Builder;
using Extension.Confluent.Kafka.Client.Health;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.IntegrationTests
{
    [TestFixture]
    public class BasicTests
    {
        private const string bootstrapServers = "localhost:29092";

        private IAdminClient adminClient;
        private AdminClientBuilder adminClientBuilder;
        private IProducer<string, Null> producer;
        private IBufferedConsumer<string, Null> consumer;
        private ILogger logger;

        private string topicName;

        [OneTimeSetUp]
        public void Init()
        {
            logger = new Mock<ILogger>().Object;
            adminClientBuilder = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers });
            adminClient = adminClientBuilder.Build();
            producer = new ProducerBuilder<string, Null>(new ProducerConfig { BootstrapServers = bootstrapServers, BatchNumMessages = 1 })
                .Build();
        }

        //[OneTimeTearDown]
        //public void Cleanup()
        //{
        //    adminClient.Dispose();
        //    producer.Dispose();
        //}

        [SetUp]
        public void Setup()
        {
            topicName = $"TestTopic-{DateTime.UtcNow.ToString("yy_MM_dd_HH_mm_ss")}_{TestContext.CurrentContext.Test.ID}";

            adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = topicName, NumPartitions = 1 }
            });   
        }

        [Test]
        public void Consume_SingleTopic_VerifyMessageSet()
        {
            using (ManualResetEvent messageReceivedEvent = new ManualResetEvent(false))
            {
                //Note: send messages on topic which are expected to be received in order
                producer.Produce(topicName, new Message<string, Null> { Key = "test-message-1" });
                producer.Produce(topicName, new Message<string, Null> { Key = "test-message-2" });
                producer.Flush(TimeSpan.FromSeconds(1));

                var callback = new Callback(2, messageReceivedEvent);

                using (consumer = new BufferedConsumerBuilder<string, Null>(new Configuration.BufferedConsumerConfig
                {
                    TopicConfigs = new[]
                  {
                    new BufferedTopicConfig
                    {
                        TopicName = topicName
                    }
                }
                })
                  .SetAdminBuilder(adminClientBuilder)
                  .SetConsumerBuilder(new ConsumerBuilder<string, Null>(new ConsumerConfig
                  {
                      GroupId = $"test-group-{TestContext.CurrentContext.Test.ID}",
                      ClientId = TestContext.CurrentContext.Test.ID,
                      BootstrapServers = bootstrapServers,
                      AutoOffsetReset = AutoOffsetReset.Earliest,
                      EnableAutoCommit = false,
                      FetchWaitMaxMs = 50
                  }))
                  .SetCallback(callback)
                  .SetHealthStatusCallback(callback)
                  .SetLogger(logger)
                  .Build())
                {
                    consumer.Subscribe();

                    //wailt until all messages are received or timeout is reached
                    messageReceivedEvent.WaitOne(TimeSpan.FromMinutes(1));

                    TestContext.Out.WriteLine("all messages received, check if received in correct order");

                    Assert.That(callback.ReceivedResults.Count, Is.EqualTo(2));
                    Assert.That(callback.ReceivedResults[0].Message.Key, Is.EqualTo("test-message-1"));
                    Assert.That(callback.ReceivedResults[1].Message.Key, Is.EqualTo("test-message-2"));

                    consumer.Unsubscribe();
                }
            }
        }

        private class Callback : IConsumeResultCallback<string, Null>, IHealthStatusCallback
        {
            private ManualResetEvent mre;
            private int messagesToWait;
            private object lockObj = new object();

            public List<ConsumeResult<string, Null>> ReceivedResults { get; init; }

            public Callback(int messagesToWait, ManualResetEvent mre)
            {
                this.mre = mre;
                this.messagesToWait = messagesToWait;
                this.ReceivedResults = new List<ConsumeResult<string, Null>>();
            }

            public void OnHealthyConnection()
            {
                TestContext.Out.WriteLine($"connection health {TestContext.CurrentContext.Test.ID}");
            }

            public void OnMessageLoopCancelled(string reason)
            {
                TestContext.Out.WriteLine($"message loop cancelled {TestContext.CurrentContext.Test.ID}");
            }

            public void OnMessageLoopPing()
            {
                TestContext.Out.WriteLine($"message loop running {TestContext.CurrentContext.Test.ID}");
            }

            public Task OnReceivedAsync(ReadOnlyMemory<ConsumeResult<string, Null>> results, CancellationToken cancellationToken)
            {
                lock(lockObj)
                {
                    ReceivedResults.AddRange(results.Span.ToArray());
                    messagesToWait -= results.Span.Length;

                    if (messagesToWait == 0)
                    {
                        //Signal that all messages are received
                        mre.Set();
                    }
                }

                return Task.CompletedTask;
            }

            public void OnUnhealthyConnection(Exception exception)
            {
                TestContext.Out.WriteLine($"connection broken {TestContext.CurrentContext.Test.ID}");
            }

            public void OnWorkerTaskCancelled(long channelId, string reason)
            {
                TestContext.Out.WriteLine($"worker task cancelled {TestContext.CurrentContext.Test.ID}");
            }
        }
    }
}