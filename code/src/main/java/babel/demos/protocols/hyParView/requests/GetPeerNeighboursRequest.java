package babel.demos.protocols.hyParView.requests;

import babel.requestreply.ProtocolRequest;
import network.Host;

import java.util.UUID;

public class GetPeerNeighboursRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 101;

    private final int fanout;
    private Host sender;
    private UUID identifier;
    private short requestType;

    public GetPeerNeighboursRequest(UUID identifier, int fanout, Host sender, short requestType) {
        super(GetPeerNeighboursRequest.REQUEST_ID);
        this.fanout = fanout;
        this.identifier = identifier;//UUID.randomUUID();
        this.sender = sender;
        this.requestType = requestType;
    }

    public int getFanout() {
        return fanout;
    }

    public UUID getIdentifier() {
        return identifier;
    }

    public Host getSenderHost(){return sender;}

    public short getRequestType(){ return requestType;}
}
