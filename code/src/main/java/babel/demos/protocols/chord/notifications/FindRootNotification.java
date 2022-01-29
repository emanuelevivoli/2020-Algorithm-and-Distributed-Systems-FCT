package babel.demos.protocols.chord.notifications;

import babel.notification.ProtocolNotification;
import network.Host;

import java.math.BigInteger;

public class FindRootNotification extends ProtocolNotification {
    public static final short NOTIFICATION_ID = 452;
    public static final String NOTIFICATION_NAME = "FindRootNotification";

    private BigInteger id;
    private Host nextNode; // node to witch the message to sent

    public FindRootNotification(Host nextNode, BigInteger id) {
        super(FindRootNotification.NOTIFICATION_ID, FindRootNotification.NOTIFICATION_NAME);
        this.id = id;
        this.nextNode = nextNode;
        System.out.println(NOTIFICATION_NAME + " | " + id + " | " + nextNode);

    }

    public Host getNextNode() {
        return nextNode;
    }


    public BigInteger getChordId() {
        return id;
    }

}
