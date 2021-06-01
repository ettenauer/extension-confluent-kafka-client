using Extension.Confluent.Kafka.Client.Configuration;
using NUnit.Framework;

namespace Extension.Confluent.Kafka.Client.Tests.Configuration
{
    [TestFixture]
    public class BufferedTopicConfigTests
    {
        [Test]
        public void BufferedConsumerConfig_Default_ExpectedValues()
        {
            var config = new BufferedTopicConfig();

            Assert.That(config.Priority, Is.EqualTo(0));
        }
    }
}
