using Confluent.Kafka;
using NUnit.Framework;
using System;
using System.Buffers;
using System.Threading.Tasks;

namespace Extension.Confluent.Kafka.Client.Tests
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public async Task Test1()
        {
            var memoryOwner = MemoryPool<ConsumeResult<byte[], byte[]>>.Shared.Rent(10);

            try
            {
                var memory = memoryOwner.Memory;

                await Task.Delay(100);

                for(int i = 0; i < 10; i++)
                {
                    memory.Span[i] = new ConsumeResult<byte[], byte[]>();
                }

                memory = memory.Slice(0, 10);
                Console.WriteLine("Length:" + memory.Length);
                for(int i = 0; i < memory.Span.Length; i++)
                {
                    Console.WriteLine("Item:" + memory.Span[i]);
                }
            }
            finally
            {
                memoryOwner.Dispose();
            }
        }
    }
}