using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Local.Runner.Examples
{
    internal class SampleProducer : IDisposable
    {
        private const string Topic = "testTopic";

        private readonly int messageCount;
        private readonly IProducer<byte[], byte[]> producer;
        private readonly ILogger logger;

        public SampleProducer(int messageCount, ILogger logger)
        {
            this.logger = logger;
            this.messageCount = messageCount;
            this.producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = "localhost:9092" })
                .Build();
        }

        public void Produce()
        {
            var tasks = new List<Task>();

            for (int i = 0; i < messageCount; ++i)
            {
                producer.Produce(Topic, new Message<byte[], byte[]> { Key = BitConverter.GetBytes(i), Value = BitConverter.GetBytes(i) });
            }   
        }

        public void Dispose()
        {
            producer.Flush(TimeSpan.FromSeconds(5));
            producer.Dispose();
        }
    }
}
