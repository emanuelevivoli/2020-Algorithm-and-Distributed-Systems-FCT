package babel.demos.protocols.scribe.requests;

import babel.requestreply.ProtocolRequest;

public class DisseminateMessageRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 701;

    private String requestType; // PUB, SUB, UNSUB
    private String topic;
    private byte[] payload;

    public DisseminateMessageRequest(String requestType, String topic, byte[] message) {
        super(DisseminateMessageRequest.REQUEST_ID);
        this.requestType = requestType;
        this.topic = topic;
        if(message != null) {
            this.payload = new byte[message.length];
            System.arraycopy(message, 0, this.payload, 0, message.length);
        } else {
            this.payload = new byte[0];
        }
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public String getRequestType() {
        return requestType;
    }
}
