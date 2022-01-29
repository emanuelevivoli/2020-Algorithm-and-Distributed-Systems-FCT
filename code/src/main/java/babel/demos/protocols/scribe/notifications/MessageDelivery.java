package babel.demos.protocols.scribe.notifications;

import babel.notification.ProtocolNotification;

public class MessageDelivery extends ProtocolNotification {
    public static final short NOTIFICATION_ID = 711;
    public static final String NOTIFICATION_NAME = "MessageDeliveryNotification";

    private String topic;
    private byte[] message;

    public MessageDelivery(String topic, byte[] message) {
        super(MessageDelivery.NOTIFICATION_ID, MessageDelivery.NOTIFICATION_NAME);

        this.topic = topic;

        if(message != null) {
            this.message = new byte[message.length];
            System.arraycopy(message, 0, this.message, 0, message.length);
        } else {
            this.message = new byte[0];
        }
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getMessage() {
        return message;
    }
}
