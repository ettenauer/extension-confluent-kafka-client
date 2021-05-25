using Confluent.Kafka;
using System;

namespace Extension.Confluent.Kafka.Client.Extensions
{
    public static class HeaderExtensions
    {
        public static Headers AddWorkerChannelId(this Headers headers, long channelId)
        {
            headers.Add(new Header(HeaderFields.WorkerChannelId, BitConverter.GetBytes(channelId)));
            return headers;
        }

        public static long? GetWorkerChannelId(this Headers headers)
        {
            headers.TryGetLastBytes(HeaderFields.WorkerChannelId, out var bytes);
            return bytes.Length == 4 ? BitConverter.ToInt64(bytes) : (long?) null;
        }

        public static Headers AddTopicPriority(this Headers headers, byte priority)
        {
            headers.Add(new Header(HeaderFields.TopicPriority, BitConverter.GetBytes(priority)));
            return headers;
        }

        public static byte? GetTopicPriority(this Headers headers)
        {
            headers.TryGetLastBytes(HeaderFields.WorkerChannelId, out var bytes);
            return bytes.Length == 1 ? bytes[0] : (byte?) null;
        }
    }
}
