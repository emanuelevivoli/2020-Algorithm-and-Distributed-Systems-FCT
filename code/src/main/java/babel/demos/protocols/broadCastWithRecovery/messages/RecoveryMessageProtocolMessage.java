package babel.demos.protocols.broadCastWithRecovery.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.util.UUID;

public class RecoveryMessageProtocolMessage extends ProtocolMessage {

    public static final short MSG_CODE = 205;

    private final UUID mid;
    private byte[] payload;
    private long timeStamp;

    public RecoveryMessageProtocolMessage(UUID mid, byte[] payload, long timeStamp) {
        super(RecoveryMessageProtocolMessage.MSG_CODE);
        this.mid = mid;
        this.payload = payload;
        this.timeStamp = timeStamp;
    }

    public void setPayload(byte[] payload) {
        this.payload = new byte[payload.length];
        System.arraycopy(payload, 0, this.payload, 0, payload.length);
    }

    public int getLength() {
        return this.payload.length;
    }

    public byte[] getPayload() {
        return this.payload;
    }

    public UUID getMessageId() {
        return mid;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public static final ISerializer<RecoveryMessageProtocolMessage> serializer = new ISerializer<RecoveryMessageProtocolMessage>() {
        @Override
        public void serialize(RecoveryMessageProtocolMessage m, ByteBuf out) {
            out.writeLong(m.getTimeStamp());
            out.writeLong(m.getMessageId().getMostSignificantBits());
            out.writeLong(m.getMessageId().getLeastSignificantBits());
            out.writeInt(m.getPayload().length);
            out.writeBytes(m.getPayload());

        }

        @Override
        public RecoveryMessageProtocolMessage deserialize(ByteBuf in) {
            long timer = in.readLong();
            UUID mid = new UUID(in.readLong(), in.readLong());
            int size = in.readInt();
            byte[] payload = new byte[size];
            in.readBytes(payload);
            return new RecoveryMessageProtocolMessage(mid, payload, timer);
        }

        @Override
        public int serializedSize(RecoveryMessageProtocolMessage m) {
            return (2 * Long.BYTES) + Integer.BYTES + m.payload.length;
        }
    };
}
