package io.confluent.kafka.connect;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class UdpSinkTask extends NettySinkTask {

  @Override
  protected Channel createWorkerChannel(InetAddress targetAddress, Integer port, EventLoopGroup bossGroup, EventLoopGroup workerGroup, ChannelInitializer channelInitializer) {
    InetSocketAddress target = new InetSocketAddress(targetAddress, port);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
        .handler(channelInitializer)
        .channel(NioDatagramChannel.class);

    return bootstrap.connect(target).channel();
  }

  @Override
  protected Class<? extends NettySinkChannelInitializer> getDefaultChannelInitializerClass() {
    return BytesUdpChannelInitializer.class;
  }

}
