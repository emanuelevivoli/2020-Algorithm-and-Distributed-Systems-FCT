package babel.demos.protocols.chord.messages;

import babel.demos.protocols.chord.utils.ChordNode;
import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;

public class PredecessorReplyProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 502;
    private final ChordNode newNode;

    public PredecessorReplyProtocolMessage(ChordNode newNode) {
        super(PredecessorReplyProtocolMessage.MSG_CODE);
        this.newNode = newNode;
    }

    public ChordNode getNewNode() {
        return newNode;
    }

    @Override
    public String toString() {
        return "FindPredecessorProtocolMessage{" +
                "newNode=" + newNode +
                '}';
    }

    public static final ISerializer<PredecessorReplyProtocolMessage> serializer = new ISerializer<PredecessorReplyProtocolMessage>() {
        @Override
        public void serialize(PredecessorReplyProtocolMessage joinMessage, ByteBuf out) {
            joinMessage.getNewNode().serialize(out);
        }

        @Override
        public PredecessorReplyProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            ChordNode node = ChordNode.deserialize(in);
            return new PredecessorReplyProtocolMessage(node);
        }

        @Override
        public int serializedSize(PredecessorReplyProtocolMessage joinMessage) {
            ChordNode newNode = joinMessage.getNewNode();
            return newNode == null ? 0 : newNode.serializedSize();
        }
    };
}
