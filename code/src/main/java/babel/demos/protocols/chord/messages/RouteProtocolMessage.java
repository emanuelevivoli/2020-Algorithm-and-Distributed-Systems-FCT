package babel.demos.protocols.chord.messages;

import babel.demos.protocols.chord.Chord;
import babel.demos.protocols.chord.utils.ChordNode;
import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;

public class RouteProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 509;
    private ChordNode newNode;
    private final byte[] message;


    public RouteProtocolMessage(ChordNode newNode, byte[] message) {
        super(RouteProtocolMessage.MSG_CODE);
        this.newNode = newNode;
        this.message = message;

    }

    public ChordNode getNewNode() {
        return newNode;
    }

    public void setNewNode(ChordNode newNode) {
        this.newNode = newNode;
    }

    public byte[] getMessage() {
        return message;
    }


    @Override
    public String toString() {
        return "RouteProtocolMessage{" +
                "newNode=" + newNode +
                "message=" + message +
                '}';
    }

    public static final ISerializer<RouteProtocolMessage> serializer = new ISerializer<RouteProtocolMessage>() {

        @Override
        public void serialize(RouteProtocolMessage joinMessage, ByteBuf out) {
            out.writeInt(joinMessage.message.length);
            out.writeBytes(joinMessage.message);
            joinMessage.getNewNode().serialize(out);

        }

        @Override
        public RouteProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            int stringSize = in.readInt();
            byte[] msg = new byte[stringSize];
            in.readBytes(msg);
            ChordNode node = ChordNode.deserialize(in);
            return new RouteProtocolMessage(node, msg);
        }

        @Override
        public int serializedSize(RouteProtocolMessage joinMessage) {
            return joinMessage.getNewNode().serializedSize() + 4 + joinMessage.getMessage().length;
        }
    };
}