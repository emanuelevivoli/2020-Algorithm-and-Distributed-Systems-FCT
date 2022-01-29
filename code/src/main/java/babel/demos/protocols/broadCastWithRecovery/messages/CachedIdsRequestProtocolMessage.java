package babel.demos.protocols.broadCastWithRecovery.messages;

import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolRequest;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

public class CachedIdsRequestProtocolMessage extends ProtocolMessage {

    public static final short MSG_CODE = 203;

    private Set<UUID> cachedIds;

    public CachedIdsRequestProtocolMessage(Set<UUID> cachedIds) {
        super(CachedIdsRequestProtocolMessage.MSG_CODE);
        this.cachedIds = cachedIds;
    }

    public Set<UUID> getCachedIds() {
        return cachedIds;
    }

    public static final ISerializer<CachedIdsRequestProtocolMessage> serializer = new ISerializer<CachedIdsRequestProtocolMessage>() {
        @Override
        public void serialize(CachedIdsRequestProtocolMessage m, ByteBuf out) {
            Set<UUID> notReceivedIds = m.getCachedIds();
            out.writeInt(notReceivedIds.size());

            Iterator<UUID> it = notReceivedIds.iterator();
            while (it.hasNext()) {
                UUID id = it.next();
                out.writeLong(id.getMostSignificantBits());
                out.writeLong(id.getLeastSignificantBits());
            }
        }

        @Override
        public CachedIdsRequestProtocolMessage deserialize(ByteBuf in) {
            int size = in.readInt();
            Set<UUID> ret = new HashSet<>();

            for (int i = 0; i < size; i++) {
                ret.add(new UUID(in.readLong(), in.readLong()));
            }

            return new CachedIdsRequestProtocolMessage(ret);
        }

        @Override
        public int serializedSize(CachedIdsRequestProtocolMessage m) {
            return 4 + m.getCachedIds().size() * (2 * Long.BYTES);
        }
    };


}
