package babel.demos.protocols.hyParView.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;

public class DisconnectProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 4;
    private final Host peer;

    public DisconnectProtocolMessage(Host peer) {
        super(DisconnectProtocolMessage.MSG_CODE);
        this.peer = peer;
    }

    public Host getPeer() {
        return peer;
    }

    @Override
    public String toString() {
        return "DisconnectMessage{" +
                "peer=" + peer +
                '}';
    }

    public static final ISerializer<DisconnectProtocolMessage> serializer = new ISerializer<DisconnectProtocolMessage>() {
        @Override
        public void serialize(DisconnectProtocolMessage disconnectMessage, ByteBuf out) {
            disconnectMessage.peer.serialize(out);
        }

        @Override
        public DisconnectProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host peer = Host.deserialize(in);
            return new DisconnectProtocolMessage(peer);
        }

        @Override
        public int serializedSize(DisconnectProtocolMessage disconnectMessage) {
            return disconnectMessage.peer.serializedSize();
        }
    };
}
