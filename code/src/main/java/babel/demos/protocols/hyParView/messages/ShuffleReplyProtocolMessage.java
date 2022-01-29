package babel.demos.protocols.hyParView.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.Host;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class ShuffleReplyProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 7;

    private final List<Host> sample;
    private volatile int size = -1;


    public ShuffleReplyProtocolMessage(List<Host> sample) {
        super(ShuffleReplyProtocolMessage.MSG_CODE);
        this.sample = sample;
    }

    @Override
    public String toString() {
        return "ShuffleReplyMessage{" +
                "sample=" + sample +
                '}';
    }

    public List<Host> getSample() {
        return sample;
    }

    public static final ISerializer<ShuffleReplyProtocolMessage> serializer = new ISerializer<ShuffleReplyProtocolMessage>() {
        @Override
        public void serialize(ShuffleReplyProtocolMessage shuffleReplyMessage, ByteBuf out) {
            out.writeShort(shuffleReplyMessage.sample.size());
            for (Host h : shuffleReplyMessage.sample) {
                h.serialize(out);
            }
        }

        @Override
        public ShuffleReplyProtocolMessage deserialize(ByteBuf in) throws UnknownHostException {
            short size = in.readShort();
            List<Host> payload = new LinkedList<>();
            for (short i = 0; i < size; i++) {
                payload.add(Host.deserialize(in));
            }
            return new ShuffleReplyProtocolMessage(payload);
        }

        @Override
        public int serializedSize(ShuffleReplyProtocolMessage shuffleReplyMessage) {
            if (shuffleReplyMessage.size == -1) {
                shuffleReplyMessage.size = 2; //short size
                for (Host h : shuffleReplyMessage.sample) {
                    shuffleReplyMessage.size += h.serializedSize();
                }
            }
            return shuffleReplyMessage.size;
        }
    };
}
