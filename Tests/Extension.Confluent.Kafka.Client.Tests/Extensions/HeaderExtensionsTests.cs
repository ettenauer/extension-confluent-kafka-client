using Confluent.Kafka;
using Extension.Confluent.Kafka.Client.Extensions;
using NUnit.Framework;
using System;

namespace Extension.Confluent.Kafka.Client.Tests.Extensions
{
    [TestFixture]
    public class HeaderExtensionsTests
    {
        private Headers headers;
        
        [SetUp]
        public void SetUp()
        {
            headers = new Headers();
        }

        [Test]
        public void AddWorkerChannelId_EmptyHeaders_ExpectNewHeader()
        {
            headers.AddWorkerChannelId(2);

            Assert.That(headers.GetWorkerChannelId(), Is.EqualTo(2));
        }

        [Test]
        public void AddTopicPriority_EmptyHeaders__ExpectNewHeader()
        {
            headers.AddTopicPriority(2);

            Assert.That(headers.GetTopicPriority(), Is.EqualTo(2));
        }

        [Test]
        public void GetWorkerChannelId_EmptyHeaders_ReturnNull()
        {
            Assert.That(headers.GetWorkerChannelId(), Is.Null);
        }

        [Test]
        public void GetWorkerChannelId_InvalidValue_ReturnNull()
        {
            //Note: invalid expect long
            headers.Add(HeaderFields.WorkerChannelId, BitConverter.GetBytes((short)1));

            Assert.That(headers.GetWorkerChannelId(), Is.Null);
        }

        [Test]
        public void GetTopicPriority_EmptyHeaders_ReturnNull()
        {
            //Note: invalid expect byte
            headers.Add(HeaderFields.TopicPriority, BitConverter.GetBytes((long)1));

            Assert.That(headers.GetTopicPriority(), Is.Null);
        }
    }
}
