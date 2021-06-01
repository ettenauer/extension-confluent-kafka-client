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
            if(headers.TryGetLastBytes(HeaderFields.WorkerChannelId, out var bytes))
            {
                return bytes.Length == 8 ? BitConverter.ToInt64(bytes) : (long?)null;
            }

            return null;
        }

        public static Headers AddTopicPriority(this Headers headers, byte priority)
        {
            headers.Add(new Header(HeaderFields.TopicPriority, new byte[] { priority }));
            return headers;
        }

        public static byte? GetTopicPriority(this Headers headers)
        {
            if (headers.TryGetLastBytes(HeaderFields.TopicPriority, out var bytes))
            {
                return bytes.Length == 1 ? bytes[0] : (byte?)null;
            }

            return null;
        }
    }
}
