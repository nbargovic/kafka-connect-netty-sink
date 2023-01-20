package io.confluent.kafka.connect;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.AddressedEnvelope;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.DatagramPacket;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.junit.jupiter.api.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class BytesValueRecordHandlerTest {

    InetSocketAddress remoteAddress = new InetSocketAddress("127.0.0.1", 5000);
    EmbeddedChannel channel;
    byte[] stringBytes;

    @BeforeEach
    void setup() {
        channel = new EmbeddedChannel(new BytesValueRecordHandler(remoteAddress, null, null, null, false));
        stringBytes = "Test Bytes".getBytes(StandardCharsets.UTF_8);
    }

    @AfterEach
    void teardown() {
        channel = null;
    }

    @Test
    void nullValueOutputsNothing() {

        SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 1L);

        // fails because there is no output
        assertFalse(channel.writeOutbound(record));
        assertFalse(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        assertNull(read);
    }

    @Test
    void outputIsAddressedEnvelope() {

        SinkRecord record = new SinkRecord("test", 0, null, null, null, stringBytes, 1L);

        // write record
        assertTrue(channel.writeOutbound(record));
        assertTrue(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        assertNotNull(read);
        assertTrue(read instanceof AddressedEnvelope);
    }

    @Test
    void outputValueMatchesBytesOfInput() {

        SinkRecord record = new SinkRecord("test", 0, null, null, Schema.BYTES_SCHEMA, stringBytes, 1L);

        // write record
        assertTrue(channel.writeOutbound(record));
        assertTrue(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        ByteBuf contents = (ByteBuf) ((AddressedEnvelope<?, ?>) read).content();

        assertTrue(contents.isReadable());
        assertArrayEquals(stringBytes, contents.array());
    }

    @Test
    void handleByteBufferInValue() {
        ByteBuffer buffer = ByteBuffer.wrap(stringBytes);
        SinkRecord record = new SinkRecord("test", 0, null, null, Schema.BYTES_SCHEMA, buffer, 1L);

        // write record
        assertTrue(channel.writeOutbound(record));
        assertTrue(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        ByteBuf contents = (ByteBuf) ((AddressedEnvelope<?, ?>) read).content();

        assertTrue(contents.isReadable());
        assertArrayEquals(stringBytes, contents.array());
    }

    @Test
    void handlesNullValueSchema() {
        // implementation
        SinkRecord record = new SinkRecord("test", 0, null, null, null, stringBytes, 1L);

        // write record
        assertTrue(channel.writeOutbound(record));
        assertTrue(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        ByteBuf contents = (ByteBuf) ((AddressedEnvelope<?, ?>) read).content();

        assertTrue(contents.isReadable());
        assertArrayEquals(stringBytes, contents.array());
    }

    @Test
    void isTargetAddressPresentInEnvelope() {
 
        SinkRecord record = new SinkRecord("test", 0, null, null, null, stringBytes, 1L);

        // write record
        assertTrue(channel.writeOutbound(record));
        assertTrue(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        AddressedEnvelope<?, InetSocketAddress> envelope = (AddressedEnvelope<?, InetSocketAddress>) read;

        // check the recipient is the target
        assertEquals(envelope.recipient(), remoteAddress);
    }

    @Test
    void isSenderNullByDefault() {

        InetSocketAddress sourceAddress = new InetSocketAddress("source.example.com", 161);

        SinkRecord record = new SinkRecord("test", 0, null, null, null, stringBytes, 1L);

        // write record
        assertTrue(channel.writeOutbound(record));
        assertTrue(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        AddressedEnvelope<?, InetSocketAddress> envelope = (AddressedEnvelope<?, InetSocketAddress>) read;

        // check no sender is added
        assertNull(envelope.sender());
    }

    @Test
    void isSNMPSenderAddressAndPortFound() {
        EmbeddedChannel channel = new EmbeddedChannel(new BytesValueRecordHandler(remoteAddress, "pduRawBytes", "peerAddress", "peerPort", true));
        String recordMsg = "{\n\"peerAddress\": \"10.9.8.7\",\n  \"peerPort\": \"1337\",\n  \"securityName\": \"test-security-name\",\n  \"sysUpTime\": 307337354,\n  \"enterprise\": \"1.1.1.1.1.1.1.1\",\n  \"genericTrap\": 6,\n  \"specificTrap\": 2,\n  \"variables\": [\n    {\n      \"oid\": \"1.1.1.1.1.1.1.1.1.1.1.1.1.1\",\n      \"type\": \"octetString\",\n      \"value\": \"00:00\"\n    }\n  ],\n  \"pduBerEncoded\": \"YmVyIFRoaXMgaXMgYSB0ZXN0IG1zZwo=\",\n  \"pduRawBytes\": \"VGhpcyBpcyBhIHRlc3QgbXNnCg==\"\n}";
        InetSocketAddress sourceAddress = new InetSocketAddress("10.9.8.7", 1337);

        SinkRecord record = new SinkRecord("test", 0, null, null, null, recordMsg, 1L);

        // write record
        assertTrue(channel.writeOutbound(record));
        assertTrue(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        DatagramPacket datagram = (DatagramPacket)read;

        // check the sender is the source
        assertEquals(sourceAddress, datagram.sender());
    }

    @Test
    void isSyslogSenderAddressFound() {
        EmbeddedChannel channel = new EmbeddedChannel(new BytesValueRecordHandler(this.remoteAddress, "rawMessage", "remoteAddress", null, false));
        String recordMsg = "{ \"remoteAddress\": \"192.168.180.69\", \"rawMessage\": \"<182>Jan  5 08:35:30 test syslog msg\"}";
        InetSocketAddress sourceAddress = new InetSocketAddress("192.168.180.69", 5000);

        SinkRecord record = new SinkRecord("test", 0, null, null, null, recordMsg, 1L);

        // write record
        assertTrue(channel.writeOutbound(record));
        assertTrue(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        AddressedEnvelope<?, InetSocketAddress> envelope = (AddressedEnvelope<?, InetSocketAddress>) read;
        DatagramPacket datagram = (DatagramPacket)read;

        // check the sender is the source
        assertEquals(sourceAddress, datagram.sender());
    }

    @Test
    void isSenderNullWhenMissingPort() {

        InetSocketAddress sourceAddress = new InetSocketAddress("source.example.com", 161);

        SinkRecord record = new SinkRecord("test", 0, null, null, null, stringBytes, 1L);
        // Add only address not port
        record.headers().add(new SourceHeader("source-address", sourceAddress.getHostName()));
        // write record
        assertTrue(channel.writeOutbound(record));
        assertTrue(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        AddressedEnvelope<?, InetSocketAddress> envelope = (AddressedEnvelope<?, InetSocketAddress>) read;

        // check no sender added
        assertNull(envelope.sender());
    }

    @Test
    void unparseablePortHandled() {

        InetSocketAddress sourceAddress = new InetSocketAddress("source.example.com", 161);

        SinkRecord record = new SinkRecord("test", 0, null, null, null, stringBytes, 1L);
        record.headers().add(new SourceHeader("source-address", sourceAddress.getHostName()));
        // non-integer value for port
        record.headers().add(new SourceHeader("source-port", "not a number"));

        // write record
        assertTrue(channel.writeOutbound(record));
        assertTrue(channel.finish());

        // read out the value
        Object read = channel.readOutbound();
        AddressedEnvelope<?, InetSocketAddress> envelope = (AddressedEnvelope<?, InetSocketAddress>) read;

        // check no sender added
        assertNull(envelope.sender());
    }
}