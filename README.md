# extension-confluent-kafka-client
the library enhances https://github.com/confluentinc/confluent-kafka-dotnet by addtional features

## Multi-Threading support via buffers feature

Consumed messages from kafka are dispatched on internal buffers/channels. Messages are consumed from those buffers by tasks. The number of tasks and distribution is defined by setting called BufferSharding. Each task triggers a callback for application integration.

## Prioritized topics feature

Each defined topic can have a priority. The messages are dispatched into bufferes based on the defined topic priority. That allows to process messages from high prior topics faster in case low prio topics would occupy message buffer task. 

## Backpressure feature

Messages are disptached into bounded buffers. If the buffer capacity (defined in config) is reached, the consumer will pause consumption for TopicParitions assigned to the inbound channel.

## Connection monitoring feature

The connection status to broker is activly checked and disruptions are notifed via callback.
