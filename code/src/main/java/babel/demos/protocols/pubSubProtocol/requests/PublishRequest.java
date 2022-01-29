package babel.demos.protocols.pubSubProtocol.requests;

import babel.requestreply.ProtocolRequest;

public class PublishRequest extends ProtocolRequest {
    public static final short REQUEST_ID = 301;

    private String topic;
    private byte[] payload;

    public PublishRequest(String topic, byte[] message) {
        super(PublishRequest.REQUEST_ID);

        this.topic = topic;

        if(message != null) {
            this.payload = new byte[message.length];
            System.arraycopy(message, 0, this.payload, 0, message.length);
        } else {
            this.payload = new byte[0];
        }
    }

    public byte[] getPayload() {
        return payload;
    }

    public String getTopic() {
        return topic;
    }
}
