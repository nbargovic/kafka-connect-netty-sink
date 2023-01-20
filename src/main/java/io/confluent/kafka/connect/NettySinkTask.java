package io.confluent.kafka.connect;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class NettySinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(NettySinkTask.class);
  protected NettySinkConnectorConfig connConfig;
  protected String taskName = "NettySinkTask";
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  //private ChannelGroup channelGroup;
  private Channel workerChannel;
  private ChannelInitializer<Channel> channelInitializer;
  private AtomicBoolean stop;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    log.info("Starting NettySinkTask...");
    try {
      connConfig = new NettySinkConnectorConfig(props);
      final Integer port = connConfig.getInt(NettySinkConnectorConfig.TARGET_PORT_CONFIG);
      if (port == null) {
        throw new ConnectException("Target Port is not configured.");
      }

      String target = connConfig.getString(NettySinkConnectorConfig.TARGET_ADDRESS_CONFIG);
      final InetAddress targetAddress = InetAddress.getByName(target);

      // configure task representation String
      this.taskName = this.getClass().getSimpleName() + " (target: " +
          target + " port:" + port + ")";

      Class<?> chInitClass = getDefaultChannelInitializerClass();

      channelInitializer = (ChannelInitializer) Utils.newInstance(chInitClass);
      if (channelInitializer instanceof Configurable) {
        ((Configurable) channelInitializer).configure(props);
      }

      this.bossGroup = new NioEventLoopGroup();
      this.workerGroup = new NioEventLoopGroup();

      this.workerChannel = createWorkerChannel(targetAddress, port, this.bossGroup, this.workerGroup, channelInitializer);

    } catch (Exception e) {
      throw new ConnectException("NettySinkTask failed to start for topic: ", e);
    }

    stop = new AtomicBoolean(false);
    log.info("{} started.", this.taskName);

  }

  @Override
  public void put(Collection<SinkRecord> records) {
    // Naive
    records.forEach(record -> {
      log.trace("Writing record {}", record);
      this.workerChannel.writeAndFlush(record).addListener(new GenericFutureListener<Future<? super Void>>() {
        @Override
        public void operationComplete(Future<? super Void> future) {
          if (future.isSuccess()) {
            log.trace("UDP Datagram successfully sent.");
          } else {
            log.error("UDP Datagram failed.", future.cause());
          }
        }
      });
    });
  }

  protected abstract Class<? extends ChannelInitializer> getDefaultChannelInitializerClass();

  protected abstract Channel createWorkerChannel(InetAddress bindAddress, Integer port, EventLoopGroup bossGroup, EventLoopGroup workerGroup, ChannelInitializer pipelineFactory);

  @Override
  public void stop() {
    log.debug("Stopping {}", this.taskName);
    stop.set(true);
    if (this.channelInitializer instanceof Closeable) {
      try {
        ((Closeable) (this.channelInitializer)).close();
      } catch (IOException e) {
        log.warn("Failed to close pipeline factory", e);
      }
    }

    this.workerChannel.close().awaitUninterruptibly(10000);
    log.debug("Worker Channel shut down: {}", this.taskName);
    log.debug("Stopped {}", this.taskName);

    taskName = null;
  }
}
