package babel.demos.protocols.chord.messages;

import babel.demos.protocols.chord.utils.ChordNode;
import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;

public class NotifyProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 504;
    private final ChordNode newNode;

    public NotifyProtocolMessage(ChordNode newNode) {
        super(NotifyProtocolMessage.MSG_CODE);
        this.newNode = newNode;
    }

    public ChordNode getNewNode() {
        return newNode;
    }

    @Override
    public String toString() {
        return "NotifyProtocolMessage{" +
                "newNode=" + newNode +
                '}';
    }

    public static final ISerializer<NotifyProtocolMessage> serializer = new ISerializer<NotifyProtocolMessage>() {
        @Override
        public void serialize(NotifyProtocolMessage joinMessage, ByteBuf out) {
            joinMessage.getNewNode().serialize(out);
        }

        @Override
        public NotifyProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            ChordNode node = ChordNode.deserialize(in);
            return new NotifyProtocolMessage(node);
        }

        @Override
        public int serializedSize(NotifyProtocolMessage joinMessage) {
            return joinMessage.getNewNode().serializedSize();
        }
    };
}
