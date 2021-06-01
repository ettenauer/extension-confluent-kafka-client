using Extension.Confluent.Kafka.Client.Configuration;
using NUnit.Framework;

namespace Extension.Confluent.Kafka.Client.Tests.Configuration
{
    [TestFixture]
    public class BufferedConsumerConfigTests
    {
        [Test]
        public void BufferedConsumerConfig_Default_ExpectedValues()
        {
            var config = new BufferedConsumerConfig();

            Assert.That(config.BufferSharding, Is.EqualTo(BufferSharding.Parition));
            Assert.That(config.BufferMaxTaskCount, Is.EqualTo(1));
            Assert.That(config.BufferSizePerChannel, Is.EqualTo(100));
            Assert.That(config.CallbackResultCount, Is.EqualTo(10));
            Assert.That(config.ConnectionTimeoutInMilliseconds, Is.EqualTo(8000));
            Assert.That(config.BufferCommitIntervalInMilliseconds, Is.EqualTo(1000));
            Assert.That(config.BackpressurePartitionPauseInMilliseconds, Is.EqualTo(1000));
            Assert.That(config.PingIntervalInMilliseconds, Is.EqualTo(5000));
        }
    }
}
