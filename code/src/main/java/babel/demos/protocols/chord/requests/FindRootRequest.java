package babel.demos.protocols.chord.requests;

import babel.demos.utils.Hash;
import babel.requestreply.ProtocolRequest;

import java.math.BigInteger;

public class FindRootRequest extends ProtocolRequest {
    public static final short REQUEST_ID = 302;
    public static final String NOTIFICATION_NAME = "FindRootRequest";

    private BigInteger chordId;

    public FindRootRequest(BigInteger chordId) {
        super(FindRootRequest.REQUEST_ID);
        this.chordId = chordId;
        System.out.println(NOTIFICATION_NAME + " | " + chordId );
    }

    public BigInteger getChordId() {
        return chordId;
    }

}
