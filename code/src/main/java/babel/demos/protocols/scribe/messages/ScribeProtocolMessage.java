package babel.demos.protocols.scribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class ScribeProtocolMessage extends ProtocolMessage {

    public static final short MSG_CODE = 702;

    private String requestType; // SUB, PUB, UNSUB
    private String topic;
    private byte[] message;

    public ScribeProtocolMessage(String requestType, String topic, byte[] message) {
        super(ScribeProtocolMessage.MSG_CODE);
        this.requestType = requestType;
        this.topic = topic;
        this.message = message;
    }

    public byte[] getMessage() {
        return message;
    }

    public String getRequestType() {
        return requestType;
    }

    public String getTopic() {
        return topic;
    }

    public static byte[] serialize(ScribeProtocolMessage m) {
        byte[] result = new byte[serializedSize(m)];
        ByteBuffer out = ByteBuffer.wrap(result);
        out.putInt(m.requestType.length());
        out.putInt(m.topic.length());
        out.putInt(m.message.length);

        out.put(m.requestType.getBytes());
        out.put(m.topic.getBytes());
        out.put(m.message);

        return result;
    }

    public static ScribeProtocolMessage deserialize(byte[] input) {
        ByteBuffer in = ByteBuffer.wrap(input);

        int requestTypeLength = in.getInt();
        int topicLength = in.getInt();
        int messageLength = in.getInt();

        byte[] requestTypeBytes = new byte[requestTypeLength];
        in.get(requestTypeBytes);
        String requestType = new String(requestTypeBytes); // SUB, PUB, UNSUB

        byte[] topicBytes = new byte[topicLength];
        in.get(topicBytes);
        String topic = new String(topicBytes);

        byte[] message = new byte[messageLength];
        in.get(message);

        return new ScribeProtocolMessage(requestType, topic, /*originalHost,*/ message);
    }

    public static int serializedSize(ScribeProtocolMessage m) {
        return 4 * 3 + m.getRequestType().length() + m.getTopic().length() + m.getMessage().length;
    }


    public static final ISerializer<ScribeProtocolMessage> serializer = new ISerializer<ScribeProtocolMessage>() {
        @Override
        public void serialize(ScribeProtocolMessage m, ByteBuf out) {
            out.writeInt(m.requestType.length());
            out.writeInt(m.topic.length());
            out.writeInt(m.message.length);

            out.writeBytes(m.requestType.getBytes());
            out.writeBytes(m.topic.getBytes());
            out.writeBytes(m.message);
        }

        @Override
        public ScribeProtocolMessage deserialize(ByteBuf in) {
            int requestTypeLength = in.readInt();
            int topicLength = in.readInt();
            int messageLength = in.readInt();

            byte[] requestTypeBytes = new byte[requestTypeLength];
            in.readBytes(requestTypeBytes);
            String requestType = new String(requestTypeBytes); // SUB, PUB, UNSUB

            byte[] topicBytes = new byte[topicLength];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes);

            byte[] message = new byte[messageLength];
            in.readBytes(message);

            return new ScribeProtocolMessage(requestType, topic, /*originalHost,*/ message);

        }

        @Override
        public int serializedSize(ScribeProtocolMessage m) {
            return 4 * 3 + m.getRequestType().length() + m.getTopic().length() /*+ m.getOriginalSender().serializedSize()*/ + m.getMessage().length;
        }
    };
}
