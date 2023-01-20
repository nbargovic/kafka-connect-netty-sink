package io.confluent.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

public class NettySinkConnectorConfig extends AbstractConfig {

  public static final String TARGET_ADDRESS_CONFIG = "target.address";
  public static final String TARGET_ADDRESS_DEFAULT = "localhost";

  public static final String TARGET_PORT_CONFIG = "target.port";

  public static final String TRANSPORT_PROTOCOL_CONFIG = "transport.protocol";
  public static final String TRANSPORT_PROTOCOL_DEFAULT = "udp";

  public static final String BYTES_LOCATION_CONFIG = "kafka.msg.bytes.fieldname";
  public static final String SOURCE_ADDRESS_LOCATION_CONFIG = "kafka.msg.source.address.fieldname";
  public static final String SOURCE_PORT_LOCATION_CONFIG = "kafka.msg.source.port.fieldname";
  public static final String DECODE_BYTES_CONFIG = "kafka.msg.decode.bytes";

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public NettySinkConnectorConfig(Map<String, ?> props) {
    super(CONFIG_DEF, props);
  }

  protected static ConfigDef baseConfigDef() {
    final ConfigDef configDef = new ConfigDef();

    final String group = "NettySinkConnector";
    int order = 0;
    configDef
        .define(TARGET_ADDRESS_CONFIG, Type.STRING, TARGET_ADDRESS_DEFAULT, Importance.HIGH, "Target address", group,
            ++order, Width.LONG, "Target address")
        .define(TARGET_PORT_CONFIG, Type.INT, null, Importance.HIGH, "Target port", group, ++order, Width.LONG,
            "Target port")
        .define(TRANSPORT_PROTOCOL_CONFIG, Type.STRING, TRANSPORT_PROTOCOL_DEFAULT, Importance.HIGH, "Type of transport: UDP", group, ++order, Width.LONG,
            "Type of transport: UDP")
        .define(SOURCE_ADDRESS_LOCATION_CONFIG, Type.STRING, null, Importance.HIGH, "Location of the Source IP", group, ++order, Width.LONG,
            "Location of the Source IP")
        .define(SOURCE_PORT_LOCATION_CONFIG, Type.STRING, null, Importance.HIGH, "Location of the Source Port", group, ++order, Width.LONG,
            "Location of the Source Port")    
        .define(BYTES_LOCATION_CONFIG, Type.STRING, null, Importance.HIGH, "Location of the raw bytes/message", group, ++order, Width.LONG,
            "Location of the raw bytes/message")        
        .define(DECODE_BYTES_CONFIG, Type.BOOLEAN, false, Importance.HIGH, "Convert bytes from base64 to raw bytes", group, ++order, Width.LONG,
            "Convert bytes from base64 to raw bytes");

    return configDef;
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toEnrichedRst());
  }
}
