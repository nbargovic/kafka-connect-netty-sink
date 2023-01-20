package io.confluent.kafka.connect;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import org.apache.kafka.common.Configurable;

import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public abstract class NettySinkChannelInitializer extends ChannelInitializer<Channel> implements Configurable, Closeable {

  protected NettySinkConnectorConfig config;

  public abstract LinkedHashMap<String, ChannelHandler> defaultHandlers(NettySinkConnectorConfig conf);

  @Override
  protected void initChannel(Channel ch) {
    final ChannelPipeline pipeline = ch.pipeline();

    configureHandlers(config, pipeline);

  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.config = new NettySinkConnectorConfig(configs);
  }

  @Override
  public void close() {
  }

  public void configureHandlers(NettySinkConnectorConfig config, ChannelPipeline pipeline) {

    // configure from defaults
    for (Entry<String, ChannelHandler> e : defaultHandlers(config).entrySet()) {
      pipeline.addLast(e.getKey(), e.getValue());
    }

  }

}
