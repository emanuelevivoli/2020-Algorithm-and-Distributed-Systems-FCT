package babel.demos.protocols.pubSubProtocol.requests;

import babel.requestreply.ProtocolRequest;

public class UnsubscribeRequest extends ProtocolRequest {
    public static final short REQUEST_ID = 303;

    private String topic;

    public UnsubscribeRequest(String topic) {
        super(UnsubscribeRequest.REQUEST_ID);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
