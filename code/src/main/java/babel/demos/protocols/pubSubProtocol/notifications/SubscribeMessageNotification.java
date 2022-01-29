package babel.demos.protocols.pubSubProtocol.notifications;

import babel.notification.ProtocolNotification;

public class SubscribeMessageNotification extends ProtocolNotification {
    public static final short NOTIFICATION_ID = 401;
    public static final String NOTIFICATION_NAME = "SubscribeMessageNotification";

    private byte[] message;

    public SubscribeMessageNotification(byte[] message) {
        super(SubscribeMessageNotification.NOTIFICATION_ID, SubscribeMessageNotification.NOTIFICATION_NAME);
        if(message != null) {
            this.message = new byte[message.length];
            System.arraycopy(message, 0, this.message, 0, message.length);
        } else {
            this.message = new byte[0];
        }
    }

    public byte[] getMessage() {
        return message;
    }
}
