package babel.demos.protocols.chord.requests;

import babel.requestreply.ProtocolRequest;

import java.math.BigInteger;

public class RouteReqRequest extends ProtocolRequest {
    public static final short REQUEST_ID = 301;

    private byte[] message;
    private BigInteger chordId;

    public RouteReqRequest(byte[] message, BigInteger id) {
        super(RouteReqRequest.REQUEST_ID);
        this.message = message;
        this.chordId = id;
    }

    public byte[] getMessage() {
        return message;
    }

    public BigInteger getChordId() {
        return chordId;
    }
}
