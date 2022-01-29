package babel.demos.protocols.broadCastWithRecovery.requests;

import babel.requestreply.ProtocolReply;

import java.util.UUID;

public class BCastReply extends ProtocolReply {

    public static final short REPLY_ID = BCastRequest.REQUEST_ID;

    private final UUID requestID;
    private byte[] payload;

    public BCastReply(UUID requestID, byte[] message) {
        super(BCastReply.REPLY_ID);
        this.requestID = requestID;
        this.payload = message;
    }

    public UUID getRequestID() {
        return requestID;
    }

    public byte[] getPayload() {
        return payload;
    }
}
