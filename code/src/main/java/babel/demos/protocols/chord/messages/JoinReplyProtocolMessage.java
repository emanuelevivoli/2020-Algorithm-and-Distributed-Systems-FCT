package babel.demos.protocols.chord.messages;

import babel.demos.protocols.chord.utils.ChordNode;
import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.math.BigInteger;
import java.net.UnknownHostException;

public class JoinReplyProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 501;
    private final ChordNode newNode;
    private final int next;
    private BigInteger id = null;
    // public JoinReplyProtocolMessage(ChordNode newNode) {
    //     super(JoinReplyProtocolMessage.MSG_CODE);
    //     this.newNode = newNode;
    // }

    public JoinReplyProtocolMessage(ChordNode newNode, int next) {
        super(JoinReplyProtocolMessage.MSG_CODE);
        this.newNode = newNode;
        this.next = next;
    }

    public JoinReplyProtocolMessage(ChordNode newNode, int next, BigInteger id) {
        super(JoinReplyProtocolMessage.MSG_CODE);
        this.newNode = newNode;
        this.next = next;
        this.id = id;
    }

    public ChordNode getNewNode() {
        return newNode;
    }

    public BigInteger getChordId() { return id;}

    public int getNext(){
        return next;
    }
    
    @Override
    public String toString() {
        return "JoinReplyProtocolMessage{" +
                "newNode=" + newNode +
                '}';
    }

    public static final ISerializer<JoinReplyProtocolMessage> serializer = new ISerializer<JoinReplyProtocolMessage>() {
        @Override
        public void serialize(JoinReplyProtocolMessage joinMessage, ByteBuf out) {
            joinMessage.getNewNode().serialize(out);
            out.writeInt(joinMessage.getNext());
        }

        @Override
        public JoinReplyProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            ChordNode node = ChordNode.deserialize(in);
            int next = in.readInt();
            return new JoinReplyProtocolMessage(node, next);
        }

        @Override
        public int serializedSize(JoinReplyProtocolMessage joinMessage) {
            return joinMessage.getNewNode().serializedSize() + 4;
        }
    };
}
