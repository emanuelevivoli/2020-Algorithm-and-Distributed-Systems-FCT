package babel.demos.protocols.broadCastWithRecovery.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

public class CachedIdsReplyProtocolMessage extends ProtocolMessage {
    public final static short MSG_CODE = 202;

    private Set<UUID> notReceivedIds;

    public CachedIdsReplyProtocolMessage(Set<UUID> notCachedIds) {
        super(CachedIdsReplyProtocolMessage.MSG_CODE);
        this.notReceivedIds = notCachedIds;
    }

    public Set<UUID> getNotReceivedIds() {
        return notReceivedIds;
    }

    public static final ISerializer<CachedIdsReplyProtocolMessage> serializer = new ISerializer<CachedIdsReplyProtocolMessage>() {
        @Override
        public void serialize(CachedIdsReplyProtocolMessage m, ByteBuf out) {
            Set<UUID> notReceivedIds = m.notReceivedIds;
            out.writeInt(notReceivedIds.size());

            Iterator<UUID> it = notReceivedIds.iterator();
            while (it.hasNext()) {
                UUID id = it.next();
                out.writeLong(id.getMostSignificantBits());
                out.writeLong(id.getLeastSignificantBits());
            }
        }

        @Override
        public CachedIdsReplyProtocolMessage deserialize(ByteBuf in) {
            int size = in.readInt();
            Set<UUID> ret = new HashSet<>();

            for (int i = 0; i < size; i++) {
                ret.add(new UUID(in.readLong(), in.readLong()));
            }

            return new CachedIdsReplyProtocolMessage(ret);
        }

        @Override
        public int serializedSize(CachedIdsReplyProtocolMessage m) {
            return 4 + m.getNotReceivedIds().size() * (2 * Long.BYTES);
        }
    };
}
