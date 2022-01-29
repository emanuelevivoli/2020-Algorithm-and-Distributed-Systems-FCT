package babel.demos.protocols.scribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.nio.ByteBuffer;

public class CountProtocolMessage extends ProtocolMessage {

    public static final short MSG_CODE = 703;

    private int count; // SUB, PUB, UNSUB
    private String topic;


    public CountProtocolMessage(int count, String topic) {
        super(CountProtocolMessage.MSG_CODE);
        this.count = count;
        this.topic = topic;
    }

    public int getCount() {
        return count;
    }

    public String getTopic() {
        return topic;
    }

    public void addCount(int count) {
        this.count = this.count + count;
    }

    public static byte[] serialize(CountProtocolMessage m) {
        byte[] result = new byte[serializedSize(m)];
        ByteBuffer out = ByteBuffer.wrap(result);
        out.putInt(m.count);
        out.putInt(m.topic.length());
        out.put(m.topic.getBytes());

        return result;
    }

    public static CountProtocolMessage deserialize(byte[] input) {
        ByteBuffer in = ByteBuffer.wrap(input);

        int count = in.getInt();
        int topicLength = in.getInt();

        byte[] topicBytes = new byte[topicLength];
        in.get(topicBytes);
        String topic = new String(topicBytes);

        return new CountProtocolMessage(count, topic);
    }

    public static int serializedSize(CountProtocolMessage m) {
        return 4 * 2 + m.getTopic().length();
    }


    public static final ISerializer<CountProtocolMessage> serializer = new ISerializer<CountProtocolMessage>() {
        @Override
        public void serialize(CountProtocolMessage m, ByteBuf out) {
            out.writeInt(m.count);
            out.writeInt(m.topic.length());
            out.writeBytes(m.topic.getBytes());
        }

        @Override
        public CountProtocolMessage deserialize(ByteBuf in) {
            int count = in.readInt();
            int topicLength = in.readInt();

            byte[] topicBytes = new byte[topicLength];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes);

            return new CountProtocolMessage(count, topic);

        }

        @Override
        public int serializedSize(CountProtocolMessage m) {
            return 4 * 2 + m.getTopic().length();
        }
    };

}