package babel.demos.protocols.hyParView.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class JoinProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 2;
    private final Host newNode;

    public JoinProtocolMessage(Host newNode) {
        super(JoinProtocolMessage.MSG_CODE);
        this.newNode = newNode;
    }

    public Host getNewNode() {
        return newNode;
    }

    @Override
    public String toString() {
        return "JoinMessage{" +
                "newNode=" + newNode +
                '}';
    }

    public static final ISerializer<JoinProtocolMessage> serializer = new ISerializer<JoinProtocolMessage>() {
        @Override
        public void serialize(JoinProtocolMessage joinMessage, ByteBuf out) {
            joinMessage.newNode.serialize(out);
        }

        @Override
        public JoinProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {

            Host payload = Host.deserialize(in);
            return new JoinProtocolMessage(payload);
        }

        @Override
        public int serializedSize(JoinProtocolMessage joinMessage) {
            return joinMessage.newNode.serializedSize();
        }
    };
}
