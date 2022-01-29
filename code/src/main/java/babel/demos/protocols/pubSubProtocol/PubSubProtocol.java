package babel.demos.protocols.pubSubProtocol;

import babel.demos.clients.AutomatedClient;
import babel.demos.protocols.broadCastWithRecovery.BroadCastWithRecovery;
import babel.demos.protocols.broadCastWithRecovery.notifications.BCastDeliver;
import babel.demos.protocols.broadCastWithRecovery.requests.BCastRequest;
import babel.demos.protocols.chord.Chord;
import babel.demos.protocols.chord.notifications.FindRootNotification;
import babel.demos.protocols.chord.requests.FindRootRequest;
import babel.demos.protocols.pubSubProtocol.messages.PubProtocolMessage;
import babel.demos.protocols.pubSubProtocol.notifications.SubscribeMessageNotification;
import babel.demos.protocols.pubSubProtocol.requests.PublishRequest;
import babel.demos.protocols.pubSubProtocol.requests.SubscribeRequest;
import babel.demos.protocols.pubSubProtocol.requests.UnsubscribeRequest;
import babel.demos.protocols.scribe.ScribeProtocol;
import babel.demos.protocols.scribe.notifications.MessageDelivery;
import babel.demos.protocols.scribe.requests.DisseminateMessageRequest;
import babel.demos.utils.Hash;
import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.exceptions.HandlerRegistrationException;
import babel.exceptions.NotificationDoesNotExistException;
import babel.exceptions.ProtocolDoesNotExist;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolNotificationHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolRequest;
import network.Host;
import network.INetwork;
import network.INodeListener;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class PubSubProtocol extends GenericProtocol implements INotificationConsumer {

    public final static short PROTOCOL_ID = 3;
    public final static String PROTOCOL_NAME = "PubSub";
    private static final int MAX_SUBSCRIBED_TOPIC = 1000;

    private Host myself;
    private Set<String> subscribedTopics;
    private HashMap<BigInteger, Host> roots;
    private HashMap<BigInteger, List<PubProtocolMessage> > pendings;

    public PubSubProtocol(INetwork net) throws HandlerRegistrationException, NotificationDoesNotExistException, ProtocolDoesNotExist {
        super(PROTOCOL_NAME, PubSubProtocol.PROTOCOL_ID, net);
        myself = net.myHost();

        //Requests
        registerRequestHandler(SubscribeRequest.REQUEST_ID, uponSubscribeRequest);
        registerRequestHandler(UnsubscribeRequest.REQUEST_ID, uponUnsubscribeRequest);
        registerRequestHandler(PublishRequest.REQUEST_ID, uponPublishRequest);

        //Notification produced
        registerNotification(SubscribeMessageNotification.NOTIFICATION_ID, SubscribeMessageNotification.NOTIFICATION_NAME);

        //Message
        registerMessageHandler(PubProtocolMessage.MSG_CODE, uponPubProtocolMessage, PubProtocolMessage.serializer);

    }

    public void addPendingMessage(BigInteger hashTopic, PubProtocolMessage msg){

        List<PubProtocolMessage> topicRoot = pendings.get(hashTopic);

        if ( topicRoot != null){

            topicRoot.add(msg);

        } else {

            topicRoot = new LinkedList<PubProtocolMessage>();
            topicRoot.add(msg);
            pendings.put(hashTopic, topicRoot);
        }
    }

    @Override
    public void init(Properties props) {
        int networkSize = Integer.parseInt(props.getProperty("SampleSize", "7"));
        subscribedTopics = new HashSet<>(MAX_SUBSCRIBED_TOPIC);
        roots = new HashMap<BigInteger, Host>(MAX_SUBSCRIBED_TOPIC);
        pendings = new HashMap<BigInteger, List<PubProtocolMessage>>(MAX_SUBSCRIBED_TOPIC);
    }

    private ProtocolRequestHandler uponSubscribeRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest r) {
            SubscribeRequest subRequest = (SubscribeRequest) r;
            subscribedTopics.add(subRequest.getTopic());

            // TODO: added phase 2
            DisseminateMessageRequest request = new DisseminateMessageRequest("SUB", subRequest.getTopic(), null);
            request.setDestination(ScribeProtocol.PROTOCOL_ID);
            try {
                sendRequest(request);
            } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
                destinationProtocolDoesNotExist.printStackTrace();
                System.exit(1);
            }
        }
    };

    private ProtocolRequestHandler uponUnsubscribeRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest r) {
            UnsubscribeRequest subRequest = (UnsubscribeRequest) r;
            subscribedTopics.remove(subRequest.getTopic());

            // TODO: added phase 2
            DisseminateMessageRequest request = new DisseminateMessageRequest("UNSUB", subRequest.getTopic(), null);
            request.setDestination(ScribeProtocol.PROTOCOL_ID);
            try {
                sendRequest(request);
            } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
                destinationProtocolDoesNotExist.printStackTrace();
                System.exit(1);
            }
        }
    };

    private ProtocolRequestHandler uponPublishRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest r) {
            PublishRequest subRequest = (PublishRequest) r;

            String topic = subRequest.getTopic();
            byte[] payload = subRequest.getPayload();

            // TODO: modification phase 2
            /*ByteBuffer buf = ByteBuffer.allocate(4 * 2 + topic.length() + payload.length);
            buf.putInt(topic.length());
            buf.putInt(payload.length);
            buf.put(topic.getBytes());
            buf.put(payload);


            BCastRequest request = new BCastRequest(buf.array());
            request.setDestination(BroadCastWithRecovery.PROTOCOL_ID);*/
            BigInteger hashTopic = Hash.getHash(myself.toString());
            Host topicRoot = roots.get(hashTopic);
            PubProtocolMessage msg = new PubProtocolMessage(topic, payload);

            if ( topicRoot != null){

                sendMessage(msg, topicRoot);

            } else {

                addPendingMessage(hashTopic, msg);

                FindRootRequest request = new FindRootRequest(hashTopic);
                // -------------- DisseminateMessageRequest request = new DisseminateMessageRequest("PUB", topic, payload);
                // -------------- request.setDestination(ScribeProtocol.PROTOCOL_ID);
                request.setDestination(Chord.PROTOCOL_ID);

                try {
                    sendRequest(request);
                } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
                    destinationProtocolDoesNotExist.printStackTrace();
                    System.exit(1);
                }
            }
        }
    };

    @Override
    public void deliverNotification(ProtocolNotification notification) {
        /*BCastDeliver deliver = (BCastDeliver) notification;*/
        if(notification instanceof MessageDelivery) {
            MessageDelivery deliver = (MessageDelivery) notification;
            byte[] message = deliver.getMessage();

           /* ByteBuffer wrapped = ByteBuffer.wrap(message);

            int topicLength = wrapped.getInt();
            int payloadLength = wrapped.getInt();

            byte[] topic = new byte[topicLength];
            wrapped.get(topic, 0, topicLength);

            byte[] payload = new byte[payloadLength];
            wrapped.get(payload, 0, payloadLength);*/


            //logger.info("Topic: " + new String(topic) + "\t Payload: " + new String(payload) + "\t Subscribed: " + subscribedTopics.contains(new String(topic)));

            //if (subscribedTopics.contains(new String(topic))) {
            //Deliver messages
            SubscribeMessageNotification subNotification = new SubscribeMessageNotification(/*payload*/message);
            triggerNotification(subNotification);
            System.out.println("SubscribeMessageNotification - "+ message );
            //}
        } else {

            FindRootNotification deliver = (FindRootNotification) notification;
            BigInteger hashTopic = deliver.getChordId();
            Host topicRoot = deliver.getNextNode();

            System.out.println("FindRootNotification - "+ hashTopic + " - "+topicRoot);

            roots.put(hashTopic, topicRoot);

            sendAllPendingMessage(hashTopic);

        }
    }

    public void sendAllPendingMessage(BigInteger hashTopic){
        Host topicRoot = roots.get(hashTopic);
        List<PubProtocolMessage> pendingMessage = pendings.get(hashTopic);
        for (PubProtocolMessage msg: pendingMessage) {
            sendMessage(msg, topicRoot);
        }
        pendings.remove(hashTopic);
    }

    private ProtocolMessageHandler uponPubProtocolMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            // TODO: remover depois
            System.out.println("JAVALI");
            String topic = ((PubProtocolMessage) msg).getTopic();
            byte[] payload = ((PubProtocolMessage) msg).getMessage();

            DisseminateMessageRequest request = new DisseminateMessageRequest("PUB", topic, payload);
            request.setDestination(ScribeProtocol.PROTOCOL_ID);

        }
    };

}
