# Kafka Connect - Netty Sink Connector

This sink connector allows Kafka Connect to send UDP data to a network endpoint.  It is implemented on top of [netty.io](https://netty.io/) (4.1).

This connector is built to read SNMP Trap and Syslog messages that were published to Kafka (as schemaless Json) using:

- [SNMP Source Connector](https://docs.confluent.io/kafka-connectors/snmp/current/overview.html#record-schema)

- [Syslog Source Connector](https://docs.confluent.io/kafka-connectors/syslog/current/overview.html#output-schema)

The output from this connector is a [DatagramPacket](https://netty.io/4.1/api/io/netty/channel/socket/DatagramPacket.html) with the byte buffer, target host, and sending host - set to the configured field values stored in the kafka message.

## Example Connect Configuration

```json
{
    "name": "netty-snmp-sink-udp",
    "config": {
      "connector.class": "io.confluent.kafka.connect.NettySinkConnector",
      "tasks.max": 1,
      "errors.tolerance": "all",
      "errors.log.enable": "true",
      "errors.log.include.messages": "true",
      "topics": "snmp-source-data",
      "target.address": "udp.example.com",
      "target.port": "2162",
      "transport.protocol": "UDP",
      "kafka.msg.source.address.fieldname": "peerAddress",
      "kafka.msg.source.port.fieldname": "peerPort",
      "kafka.msg.bytes.fieldname": "pduRawBytes",
      "kafka.msg.decode.bytes": "true",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter.schemas.enable": "false"
    }
}
```
```json
{
    "name": "netty-syslog-sink-udp",
    "config": {
      "connector.class": "io.confluent.kafka.connect.NettySinkConnector",
      "tasks.max": 1,
      "errors.tolerance": "all",
      "errors.log.enable": "true",
      "errors.log.include.messages": "true",
      "topics": "syslog-source-data",
      "target.address": "udp.example.com",
      "target.port": "5145",
      "transport.protocol": "UDP",
      "kafka.msg.bytes.fieldname": "rawMessage",
      "kafka.msg.source.address.fieldname": "remoteAddress",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter.schemas.enable": "false"
    }
}
```
