package babel.demos.protocols.chord.messages;

import babel.demos.protocols.chord.utils.ChordNode;
import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;

public class PredecessorRequestProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 503;
    private final ChordNode newNode;

    public PredecessorRequestProtocolMessage(ChordNode newNode) {
        super(PredecessorRequestProtocolMessage.MSG_CODE);
        this.newNode = newNode;
    }

    public ChordNode getNewNode() {
        return newNode;
    }

    @Override
    public String toString() {
        return "PredecessorRequestProtocolMessage{" +
                "newNode=" + newNode +
                '}';
    }

    public static final ISerializer<PredecessorRequestProtocolMessage> serializer = new ISerializer<PredecessorRequestProtocolMessage>() {
        @Override
        public void serialize(PredecessorRequestProtocolMessage joinMessage, ByteBuf out) {
            joinMessage.getNewNode().serialize(out);
        }

        @Override
        public PredecessorRequestProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            ChordNode node = ChordNode.deserialize(in);
            return new PredecessorRequestProtocolMessage(node);
        }

        @Override
        public int serializedSize(PredecessorRequestProtocolMessage joinMessage) {
            return joinMessage.getNewNode().serializedSize();
        }
    };
}
