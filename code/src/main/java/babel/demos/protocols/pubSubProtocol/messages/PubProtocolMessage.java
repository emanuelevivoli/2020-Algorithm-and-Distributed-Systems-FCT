package babel.demos.protocols.pubSubProtocol.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

public class PubProtocolMessage extends ProtocolMessage {

    public static final short MSG_CODE = 752;

    private String topic;
    private byte[] message;

    public PubProtocolMessage(String topic, byte[] message) {
        super(PubProtocolMessage.MSG_CODE);
        this.topic = topic;
        this.message = message;
    }

    public byte[] getMessage() {
        return message;
    }

    public String getTopic() {
        return topic;
    }



    public static final ISerializer<PubProtocolMessage> serializer = new ISerializer<PubProtocolMessage>() {
        @Override
        public void serialize(PubProtocolMessage m, ByteBuf out) {
            out.writeInt(m.topic.length());
            out.writeInt(m.message.length);

            out.writeBytes(m.topic.getBytes());
            out.writeBytes(m.message);
        }

        @Override
        public PubProtocolMessage deserialize(ByteBuf in) {
            int topicLength = in.readInt();
            int messageLength = in.readInt();

            byte[] topicBytes = new byte[topicLength];
            in.readBytes(topicBytes);
            String topic = new String(topicBytes);

            byte[] message = new byte[messageLength];
            in.readBytes(message);

            return new PubProtocolMessage(topic, message);

        }

        @Override
        public int serializedSize(PubProtocolMessage m) {
            return 4 * 2  + m.getTopic().length() + m.getMessage().length;
        }
    };
}
