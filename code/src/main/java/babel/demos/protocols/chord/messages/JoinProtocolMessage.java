package babel.demos.protocols.chord.messages;

import babel.demos.protocols.chord.utils.ChordNode;
import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;

public class JoinProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 500;
    private final ChordNode newNode;
    private final int next;

    // public JoinProtocolMessage(ChordNode newNode) {
    //     super(JoinProtocolMessage.MSG_CODE);
    //     this.newNode = newNode;
    //     this.next = -1;
    // }

    public JoinProtocolMessage(ChordNode newNode, int next) {
        super(JoinProtocolMessage.MSG_CODE);
        this.newNode = newNode;
        this.next = next;
    }

    public ChordNode getNewNode() {
        return newNode;
    }
    
    public int getNext(){
        return next;
    }

    @Override
    public String toString() {
        return "JoinProtocolMessage{" +
                "newNode=" + newNode +
                '}';
    }

    public static final ISerializer<JoinProtocolMessage> serializer = new ISerializer<JoinProtocolMessage>() {
        @Override
        public void serialize(JoinProtocolMessage joinMessage, ByteBuf out) {
            joinMessage.getNewNode().serialize(out);
            out.writeInt(joinMessage.getNext());
        }

        @Override
        public JoinProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            ChordNode node = ChordNode.deserialize(in);
            int next = in.readInt();
            return new JoinProtocolMessage(node, next);
        }

        @Override
        public int serializedSize(JoinProtocolMessage joinMessage) {
            return joinMessage.getNewNode().serializedSize() + 4;
        }
    };
}
