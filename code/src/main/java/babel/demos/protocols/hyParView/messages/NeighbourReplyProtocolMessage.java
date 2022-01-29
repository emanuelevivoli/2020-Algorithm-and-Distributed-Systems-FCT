package babel.demos.protocols.hyParView.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class NeighbourReplyProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 9;
    private Host sender;
    private boolean accept;

    public NeighbourReplyProtocolMessage(Host sender, boolean accept) {
        super(NeighbourReplyProtocolMessage.MSG_CODE);
        this.sender = sender;
        this.accept = accept;
    }

    public Host getSender() {
        return sender;
    }

    public boolean getAccept() {
        return accept;
    }

    @Override
    public String toString() {
        return "NeighbourRequestMessage{" +
                "sender=" + sender +
                "accept=" + accept +
                '}';
    }

    public static final ISerializer<NeighbourReplyProtocolMessage> serializer = new ISerializer<NeighbourReplyProtocolMessage>() {
        @Override
        public void serialize(NeighbourReplyProtocolMessage joinMessage, ByteBuf out) {
            out.writeBoolean(joinMessage.accept);
            joinMessage.sender.serialize(out);
        }

        @Override
        public NeighbourReplyProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            boolean accept = in.readBoolean();
            Host payload = Host.deserialize(in);
            return new NeighbourReplyProtocolMessage(payload, accept);
        }

        @Override
        public int serializedSize(NeighbourReplyProtocolMessage neighbourRequestMessage) {
            return neighbourRequestMessage.sender.serializedSize() + 1;
            //TODO maybe it's not +1 byte
        }
    };
}
