package babel.demos.protocols.pubSubProtocol.requests;

import babel.requestreply.ProtocolRequest;

public class SubscribeRequest extends ProtocolRequest {
    public static final short REQUEST_ID = 302;

    private String topic;

    public SubscribeRequest(String topic) {
        super(SubscribeRequest.REQUEST_ID);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
