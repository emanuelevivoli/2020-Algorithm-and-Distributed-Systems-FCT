package babel.demos.protocols.hyParView.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class ForwardJoinProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 3;
    private final Host newNode;
    private final short activeRandomWalk;

    public ForwardJoinProtocolMessage(Host newNode, short activeRandomWalk) {
        super(ForwardJoinProtocolMessage.MSG_CODE);
        this.newNode = newNode;
        this.activeRandomWalk = activeRandomWalk;
    }

    public Host getNewNode() {
        return newNode;
    }

    public short getActiveRandomWalk() {
        return activeRandomWalk;
    }

    @Override
    public String toString() {
        return "ForwardJoinMessage{" +
                "newNode=" + newNode +
                " activeRandomWalk=" + activeRandomWalk +
                '}';
    }

    public static final ISerializer<ForwardJoinProtocolMessage> serializer = new ISerializer<ForwardJoinProtocolMessage>() {
        @Override
        public void serialize(ForwardJoinProtocolMessage fowardJoinMessage, ByteBuf out) {
            fowardJoinMessage.newNode.serialize(out);
            out.writeShort(fowardJoinMessage.activeRandomWalk);
        }

        @Override
        public ForwardJoinProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host newNode = Host.deserialize(in);
            short activeRandomWalk = in.readShort();
            return new ForwardJoinProtocolMessage(newNode, activeRandomWalk);
        }

        @Override
        public int serializedSize(ForwardJoinProtocolMessage fowardJoinMessage) {
            int totalSize = fowardJoinMessage.newNode.serializedSize();
            totalSize += 2; //short size
            return totalSize;
        }
    };
}
