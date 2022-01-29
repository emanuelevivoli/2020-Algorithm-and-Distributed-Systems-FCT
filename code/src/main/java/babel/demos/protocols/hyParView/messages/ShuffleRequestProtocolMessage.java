package babel.demos.protocols.hyParView.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class ShuffleRequestProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 6;
    private final Host owner; //= new Host(null, 0);

    private final List<Host> sample;
    private final short passiveRandomWalk;
    private volatile int size = -1;

    public ShuffleRequestProtocolMessage(Host owner, List<Host> sample, short passiveRandomWalk) {
        super(ShuffleRequestProtocolMessage.MSG_CODE);
        this.owner = owner;
        this.sample = sample;
        this.passiveRandomWalk = passiveRandomWalk;
    }

    @Override
    public String toString() {
        return "ShuffleRequestMessage{" +
                "owner=" + owner +
                "sample=" + sample +
                "passiveRandomWalk=" + passiveRandomWalk +
                '}';
    }

    public List<Host> getSample() {
        return sample;
    }

    public Host getOwner() {
        return owner;
    }

    public short getPassiveRandomWalk() {
        return passiveRandomWalk;
    }

    public static final ISerializer<ShuffleRequestProtocolMessage> serializer = new ISerializer<ShuffleRequestProtocolMessage>() {
        @Override
        public void serialize(ShuffleRequestProtocolMessage shuffleRequestMessage, ByteBuf out) {
            shuffleRequestMessage.getOwner().serialize(out);
            out.writeShort(shuffleRequestMessage.passiveRandomWalk);
            out.writeShort(shuffleRequestMessage.sample.size());
            for (Host h : shuffleRequestMessage.sample) {
                h.serialize(out);
            }
        }

        @Override
        public ShuffleRequestProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            Host owner = Host.deserialize(in);
            short passiveRandomWalk = in.readShort();
            short size = in.readShort();
            List<Host> payload = new LinkedList<>();
            for (short i = 0; i < size; i++) {
                payload.add(Host.deserialize(in));
            }
            return new ShuffleRequestProtocolMessage(owner, payload, passiveRandomWalk);
        }

        @Override
        public int serializedSize(ShuffleRequestProtocolMessage shuffleRequestMessage) {
            if (shuffleRequestMessage.size == -1) {
                shuffleRequestMessage.size = shuffleRequestMessage.getOwner().serializedSize();
                shuffleRequestMessage.size += 2*2; // 2 shorts size
                for (Host h : shuffleRequestMessage.sample) {
                    shuffleRequestMessage.size += h.serializedSize();
                }
            }
            return shuffleRequestMessage.size;
        }
    };
}
