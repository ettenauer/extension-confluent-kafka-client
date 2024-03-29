﻿using App.Metrics;
using App.Metrics.Scheduling;
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
        private readonly IMetricsRoot metrics;
        private readonly AppMetricsTaskScheduler appMetricsTaskScheduler;

        public SampleConsumer(ILogger logger)
        {
            this.logger = logger;

            //Note: confluent configuration see https://github.com/confluentinc/confluent-kafka-dotnet
            var confluentConfig = new ConsumerConfig
            {
                GroupId = "test-group-2",
                ClientId = Environment.MachineName,
                BootstrapServers = "localhost:29092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                FetchWaitMaxMs = 50
            };

            //Note: actual configuration for buffered consumer
            var config = new BufferedConsumerConfig
            {
                BufferSharding = BufferSharding.Task,
                BufferMaxTaskCount = 5,
                BufferSizePerChannel = 500,
                TopicConfigs = new[]
                {
                    new BufferedTopicConfig
                    {
                        TopicName = Topic
                    }
                }
            };

            metrics = new MetricsBuilder()
                .Report.ToConsole()
                .Build();

            consumer = new BufferedConsumerBuilder<byte[], byte[]>(config)
                .SetConsumerBuilder(new ConsumerBuilder<byte[], byte[]>(confluentConfig))
                .SetAdminBuilder(new AdminClientBuilder(confluentConfig))
                .SetCallback(this)
                .SetHealthStatusCallback(this)
                .SetMetricsCallback(this)
                .SetChannelIdFunc((p) => p.Partition)
                .SetLogger(logger)
                .Build();

            appMetricsTaskScheduler = new AppMetricsTaskScheduler(
            TimeSpan.FromSeconds(10),
            async () =>
            {
                await Task.WhenAll(metrics.ReportRunner.RunAllAsync()).ConfigureAwait(false);
            });

            appMetricsTaskScheduler.Start();
        }

        public void Subscribe()
        {
            consumer.Subscribe();
        }

        public void OnMessageLoopPing()
        {
        }

        public void OnUnhealthyConnection(Exception exception)
        {
            logger.LogError(exception, "Connection Error:");
        }

        public void OnHealthyConnection()
        {
            logger.LogInformation("Connection healthy");
        }

        public Task OnReceivedAsync(ReadOnlyMemory<ConsumeResult<byte[], byte[]>> results, CancellationToken cancellationToken)
        {
            return Task.Delay(10, cancellationToken);
        }

        public void Dispose()
        {
            consumer.Dispose();
        }

        public void OnMessageLoopCancelled(string reason)
        {

        }

        public void OnWorkerTaskCancelled(long channelId, string reason)
        {

        }

        public void RecordSuccess(int messageCount, TimeSpan callbackDuration)
        {
            metrics.Measure.Meter.Mark(new App.Metrics.Meter.MeterOptions { Name = "Processed Message", Context = "Consumer", MeasurementUnit = Unit.Events, RateUnit = TimeUnit.Seconds }, messageCount);
        }

        public void RecordFailure(int messageCount, TimeSpan callbackDuration)
        {

        }
    }
}
