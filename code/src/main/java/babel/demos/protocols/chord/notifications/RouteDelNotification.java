package babel.demos.protocols.chord.notifications;

import babel.notification.ProtocolNotification;
import network.Host;

public class RouteDelNotification extends ProtocolNotification {
    public static final short NOTIFICATION_ID = 451;
    public static final String NOTIFICATION_NAME = "SubscribeMessageNotification";

    private boolean owner;
    private Host nextNode; // node to witch the message to sent
    private Host sender;
    private byte[] message;

    public RouteDelNotification(boolean owner, Host nextNode, Host sender, byte[] message) {
        super(RouteDelNotification.NOTIFICATION_ID, RouteDelNotification.NOTIFICATION_NAME);
        this.owner = owner;
        this.nextNode = nextNode;
        this.sender = sender;
        if(message != null) {
            this.message = new byte[message.length];
            System.arraycopy(message, 0, this.message, 0, message.length);
        } else {
            this.message = new byte[0];
        }
    }

    public Host getSender() {
        return sender;
    }

    public void setSender(Host sender) {
        this.sender = sender;
    }

    public byte[] getMessage() {
        return message;
    }

    public Host getNextNode() {
        return nextNode;
    }

    public boolean isOwner() {
        return owner;
    }
}
