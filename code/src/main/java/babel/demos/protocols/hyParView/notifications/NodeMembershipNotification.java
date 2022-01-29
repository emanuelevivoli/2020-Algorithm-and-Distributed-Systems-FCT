package babel.demos.protocols.hyParView.notifications;

import network.Host;
import babel.notification.ProtocolNotification;

import java.util.LinkedList;
import java.util.List;

public class NodeMembershipNotification extends ProtocolNotification {
    public final static short NOTIFICATION_ID = 103;
    public final static String NOTIFICATION_NAME = "membership";

    public final List<Host> membership;

    public NodeMembershipNotification(List<Host> m) {
        super(NodeMembershipNotification.NOTIFICATION_ID, NodeMembershipNotification.NOTIFICATION_NAME);
        this.membership = new LinkedList<>(m);
    }

    public List<Host> getMembership() {
        return membership;
    }

    @Override
    public String toString() {
        return "GlobalMembershipNotification{" +
                "membership=" + membership +
                '}';
    }
}
