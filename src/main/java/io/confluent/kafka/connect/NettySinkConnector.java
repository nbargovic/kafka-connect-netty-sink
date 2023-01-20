package io.confluent.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NettySinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(NettySinkConnector.class);

  protected NettySinkConnectorConfig config;
  private Class<? extends Task> taskClass;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    log.info("Start NettySinkConnector ....");
    try {
      config = new NettySinkConnectorConfig(props);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start NettySinkConnector due to configuration error", e);
    }

    String transport = config.getString(NettySinkConnectorConfig.TRANSPORT_PROTOCOL_CONFIG);
    switch (transport.toUpperCase()) {
      case "UDP":
        taskClass = UdpSinkTask.class;
        break;

      default:
        throw new ConnectException("Unsupported " + NettySinkConnectorConfig.TRANSPORT_PROTOCOL_CONFIG + " :" + transport);
    }

  }

  @Override
  public Class<? extends Task> taskClass() {
    return taskClass;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {

    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopped NettySinkConnector");
  }

  @Override
  public ConfigDef config() {
    return NettySinkConnectorConfig.CONFIG_DEF;
  }
}
