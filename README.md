# extension-confluent-kafka-client

![build](https://github.com/ettenauer/extension-confluent-kafka-client/actions/workflows/ci.yml/badge.svg?branch=main)
![integration-tests](https://github.com/ettenauer/extension-confluent-kafka-client/actions/workflows/integration-test.yml/badge.svg?branch=main)

The project adds addtional functionality to [confluent-kafka-dotnet](https://github.com/confluentinc/confluent-kafka-dotnet) by adding an abstraction layer with buffering.

### IMPORTANT ##
All additional functionalities are **restricted to a AtLeastOnce** consumpution pattern, otherwise the underlying buffering concept will cause side-effects.

## Extended Functionality

### Multi-Threading support via buffers feature

Consumed messages from kafka are dispatched on internal buffers/channels. Messages are consumed from those buffers by tasks. The number of tasks and distribution is defined by setting called BufferSharding. Each task triggers a callback for application integration.

See [here](https://github.com/ettenauer/extension-confluent-kafka-client/blob/main/Source/Extension.Confluent.Kafka.Client/Consumer/ConsumeResultDispatcher.cs)

### Prioritized topics feature

Each defined topic has a priority. The messages are dispatched into bufferes based on the defined topic priority. That allows to process messages from high prior topics faster in case low prio topics would occupy message buffer task. 

See [here](https://github.com/ettenauer/extension-confluent-kafka-client/blob/main/Source/Extension.Confluent.Kafka.Client/Consumer/ConsumeResultChannel.cs#L54)

### Backpressure feature

Messages are disptached into bounded buffers. If the buffer capacity (defined in config) is reached, the consumer will pause consumption for TopicParitions assigned to the inbound channel.

See [here](https://github.com/ettenauer/extension-confluent-kafka-client/blob/main/Source/Extension.Confluent.Kafka.Client/Consumer/BufferedConsumer.cs#L118)

### Connection monitoring feature

The connection status to broker is activly checked and disruptions are notifed via callback.

### Useage

Take a look in the [examples](Tests/Local.Runner/Examples) for usage.

```csharp

//Note: confluent configuration see https://github.com/confluentinc/confluent-kafka-dotnet
var confluentConfig = new ConsumerConfig
{
    GroupId = "test-group",
    ClientId = Environment.MachineName,
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
    };

//Note: actual configuration for buffered consumer
var config = new BufferedConsumerConfig
{
    BufferSharding = BufferSharding.Task,
    BufferMaxTaskCount = 5,
    TopicConfigs = new[]
    {
        new BufferedTopicConfig
        {
            TopicName = "testTopic"
        }
    }
};

var consumer = new BufferedConsumerBuilder<byte[], byte[]>(config)
    .SetConsumerBuilder(new ConsumerBuilder<byte[], byte[]>(confluentConfig))
    .SetAdminBuilder(new AdminClientBuilder(confluentConfig))
    .SetCallback(this)
    .SetHealthStatusCallback(this)
    .SetMetricsCallback(this)
    .SetChannelIdFunc((p) => p.Partition)
    .SetLogger(logger)
    .Build();
```

In order to process received messages ```SetCallback``` needs to be set. Here is a list of callback interfaces which can be implemented:

- [ConsumeResultCallback](https://github.com/ettenauer/extension-confluent-kafka-client/blob/main/Source/Extension.Confluent.Kafka.Client/Consumer/IConsumeResultCallback.cs)
- [HealthStatusCallback](https://github.com/ettenauer/extension-confluent-kafka-client/blob/main/Source/Extension.Confluent.Kafka.Client/Health/IHealthStatusCallback.cs)
- [MetricsCallback](https://github.com/ettenauer/extension-confluent-kafka-client/tree/main/Source/Extension.Confluent.Kafka.Client/Metrics)

## Configuration

Configuration properties and defaults can be found here [configuration](Source/Extension.Confluent.Kafka.Client/Configuration). 

## Developer Notes

The extension-confluent-kafka-client project is MIT licensed [here](LICENSE).

Please consider license of dependencies, [confluent-kafka-dotnet] (https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/LICENSE) is not MIT licensed.
