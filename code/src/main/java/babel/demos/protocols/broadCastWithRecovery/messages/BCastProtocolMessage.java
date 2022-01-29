package babel.demos.protocols.broadCastWithRecovery.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.sql.Timestamp;
import java.util.UUID;

public class BCastProtocolMessage extends ProtocolMessage {

    public final static short MSG_CODE = 201;

    private final UUID mid;
    private byte[] payload;
    private int timestamp;


    public BCastProtocolMessage(int message_ttl) {
        super(BCastProtocolMessage.MSG_CODE);
        this.mid = UUID.randomUUID();
        this.payload = new byte[0];
        this.timestamp = message_ttl;
    }

    public BCastProtocolMessage(UUID mid, byte[] payload, int timestamp) {
        super(BCastProtocolMessage.MSG_CODE);
        this.mid = mid;
        this.payload = payload;
        this.timestamp = timestamp;
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

    public int getTimeStamp(){ return timestamp;}

    public void decrementTll(){ timestamp--;}

    public static final ISerializer<BCastProtocolMessage> serializer = new ISerializer<BCastProtocolMessage>() {
        @Override
        public void serialize(BCastProtocolMessage m, ByteBuf out) {
            out.writeInt(m.timestamp);
            out.writeLong(m.mid.getMostSignificantBits());
            out.writeLong(m.mid.getLeastSignificantBits());
            out.writeInt(m.payload.length);
            out.writeBytes(m.payload);
        }

        @Override
        public BCastProtocolMessage deserialize(ByteBuf in) {
            int timestamp = in.readInt();
            UUID mid = new UUID(in.readLong(), in.readLong());
            int size = in.readInt();
            byte[] payload = new byte[size];
            in.readBytes(payload);
            return new BCastProtocolMessage(mid, payload, timestamp);
        }

        @Override
        public int serializedSize(BCastProtocolMessage m) {
            return 4 + (2*Long.BYTES) + Integer.BYTES + m.payload.length;
        }
    };
}
