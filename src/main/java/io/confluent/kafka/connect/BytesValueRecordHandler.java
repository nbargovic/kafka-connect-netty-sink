package io.confluent.kafka.connect;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DatagramPacket;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Base64;

public class BytesValueRecordHandler extends ChannelOutboundHandlerAdapter {

  static final Logger log = LoggerFactory.getLogger(BytesValueRecordHandler.class);

  private final InetSocketAddress remoteAddressPort;

  private String bytesFieldName = null;
  private String sourceAddressFieldName = null;
  private String sourcePortFieldName = null;
  private boolean decodeBytes = false;

  private ObjectMapper mapper = new ObjectMapper();

  public BytesValueRecordHandler(InetSocketAddress remoteAddressPort, 
                                 String bytesFieldName, 
                                 String sourceAddressFieldName, 
                                 String sourcePortFieldName, 
                                 boolean decodeBytes) {
    this.remoteAddressPort = remoteAddressPort;
    this.bytesFieldName = bytesFieldName;
    this.sourceAddressFieldName = sourceAddressFieldName;
    this.sourcePortFieldName = sourcePortFieldName;
    this.decodeBytes = decodeBytes;
  }


  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    log.trace("Received: " + msg.toString());

    if (msg instanceof SinkRecord) {
      SinkRecord record = (SinkRecord) msg;
      Object value = record.value();

      ByteBuf byteBuf = null;
      // The value is a byte[] and/or no fieldname is configured
      if (this.bytesFieldName == null) {
        if (value instanceof ByteBuffer) {
          byteBuf = Unpooled.wrappedBuffer((ByteBuffer) value);
        } else if (value instanceof byte[]) {
          byteBuf = Unpooled.wrappedBuffer((byte[]) value);
        }
      // We want to pull out the bytes from inside the record value
      } else {
        try {
          JsonNode json = mapper.readTree(value.toString());

          String bytesString = json.get(this.bytesFieldName).asText();
          //if base64 decode
          if (this.decodeBytes) {
            byte[] decodedBytes = Base64.getDecoder().decode(bytesString);
            byteBuf = Unpooled.copiedBuffer(decodedBytes);
          } else {
            byteBuf = Unpooled.copiedBuffer(bytesString, CharsetUtil.UTF_8);
          }
        } catch (Exception ex) {
          log.warn("Failed to find bytes field in Kafka Message", ex);
        }
      }

      if (byteBuf == null) {
        log.warn("ByteBuf is null, no data found in the kafka message.");
        // Nothing for us to do
        return;
      }

      DatagramPacket datagram = createDatagramPacket(record, byteBuf);
      ctx.writeAndFlush(datagram);
    }
  }

  /**
   * Create the Datagram Packet with the target address, and source address if present in the record.
   *
   * @param record  kafka connect record with optional headers
   * @param byteBuf byte buffer containing the bytes of the underlying record value
   * @return DatagramPacket
   */
  private DatagramPacket createDatagramPacket(SinkRecord record, ByteBuf byteBuf) {

    DatagramPacket datagram = new DatagramPacket(byteBuf, this.remoteAddressPort);

    //if a source address field name is provided, try to get the source address and port from the record
    if (this.sourceAddressFieldName != null) {
      Object value = record.value();
      try {
        JsonNode json = mapper.readTree(value.toString());
        String hostname = json.get(this.sourceAddressFieldName).asText();
        int sourcePort = this.remoteAddressPort.getPort(); //default to the target port
        if (this.sourcePortFieldName != null) {
          sourcePort = json.get(this.sourcePortFieldName).asInt(sourcePort);
        }
        if (hostname != null && !hostname.trim().isEmpty()) {
          InetSocketAddress sourceAddress = new InetSocketAddress(hostname, sourcePort);
          datagram = new DatagramPacket(byteBuf, this.remoteAddressPort, sourceAddress);
        } else {
          log.warn("Failed to find the source address and port in the SinkRecord.");
        }
      } catch (Exception ex) {
        log.warn("Failed to find the source address and port in the SinkRecord.", ex);
      }  
    }
    return datagram;
  }

}
