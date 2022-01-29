package babel.demos.protocols.hyParView.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class NeighbourRequestProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 8;
    private Host sender;

    public NeighbourRequestProtocolMessage(Host sender) {
        super(NeighbourRequestProtocolMessage.MSG_CODE);
        this.sender = sender;
    }

    public Host getSender() {
        return sender;
    }

    @Override
    public String toString() {
        return "NeighbourRequestMessage{" +
                "sender=" + sender +
                '}';
    }

    public static final ISerializer<NeighbourRequestProtocolMessage> serializer = new ISerializer<NeighbourRequestProtocolMessage>() {
        @Override
        public void serialize(NeighbourRequestProtocolMessage joinMessage, ByteBuf out) {
            joinMessage.sender.serialize(out);
        }

        @Override
        public NeighbourRequestProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host payload = Host.deserialize(in);
            return new NeighbourRequestProtocolMessage(payload);
        }

        @Override
        public int serializedSize(NeighbourRequestProtocolMessage neighbourRequestMessage) {
            return neighbourRequestMessage.sender.serializedSize();
        }
    };
}
