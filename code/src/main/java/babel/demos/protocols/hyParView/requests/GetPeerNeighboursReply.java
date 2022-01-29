package babel.demos.protocols.hyParView.requests;

import babel.requestreply.ProtocolReply;
import network.Host;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class GetPeerNeighboursReply extends ProtocolReply {
    public static final short REPLY_ID = GetPeerNeighboursRequest.REQUEST_ID;

    private final UUID requestID;
    private final List<Host> sample;
    private short replyType;

    public GetPeerNeighboursReply(UUID requestID, List<Host> sample, short replyType) {
        super(GetPeerNeighboursReply.REPLY_ID);
        this.requestID = requestID;
        this.sample = new LinkedList<>(sample);
        this.replyType = replyType;
    }

    public UUID getRequestID() {
        return requestID;
    }

    public List<Host> getSample() {
        return this.sample;
    }

    public short getReplyType() {
        return replyType;
    }
}

