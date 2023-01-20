package io.confluent.kafka.connect;

import io.netty.channel.ChannelHandler;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;

public class BytesUdpChannelInitializer extends NettySinkChannelInitializer {

  // TODO: handle PortUnreachableException in pipeline
  public LinkedHashMap<String, ChannelHandler> defaultHandlers(NettySinkConnectorConfig conf) {
    LinkedHashMap<String, ChannelHandler> defaultHandlers = new LinkedHashMap<>();

    String bytesFieldName = this.config.getString(NettySinkConnectorConfig.BYTES_LOCATION_CONFIG);
    String sourceAddressFieldName = this.config.getString(NettySinkConnectorConfig.SOURCE_ADDRESS_LOCATION_CONFIG);
    String sourcePortFieldName = this.config.getString(NettySinkConnectorConfig.SOURCE_PORT_LOCATION_CONFIG);
    boolean decodeBytes = this.config.getBoolean(NettySinkConnectorConfig.DECODE_BYTES_CONFIG);

    String targetAddress = this.config.getString(NettySinkConnectorConfig.TARGET_ADDRESS_CONFIG);
    int targetPort = this.config.getInt(NettySinkConnectorConfig.TARGET_PORT_CONFIG);
    InetSocketAddress remoteAddress = new InetSocketAddress(targetAddress, targetPort);
    defaultHandlers.put("recordHandler", new BytesValueRecordHandler(remoteAddress, bytesFieldName, sourceAddressFieldName, sourcePortFieldName, decodeBytes));
    return defaultHandlers;
  }

}
