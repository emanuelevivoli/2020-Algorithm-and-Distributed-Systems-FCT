package babel.demos.protocols.scribe;

import babel.demos.protocols.chord.Chord;
import babel.demos.protocols.chord.notifications.RouteDelNotification;
import babel.demos.protocols.chord.requests.RouteReqRequest;
import babel.demos.protocols.scribe.messages.ScribeProtocolMessage;
import babel.demos.protocols.scribe.notifications.MessageDelivery;
import babel.demos.protocols.scribe.requests.DisseminateMessageRequest;
import babel.demos.utils.Hash;
import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.exceptions.HandlerRegistrationException;
import babel.exceptions.NotificationDoesNotExistException;
import babel.exceptions.ProtocolDoesNotExist;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolRequest;
import network.Host;
import network.INetwork;
import org.apache.logging.log4j.util.SystemPropertiesPropertySource;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class ScribeProtocol extends GenericProtocol implements INotificationConsumer {

    public final static short PROTOCOL_ID = 9;
    public final static String PROTOCOL_NAME = "Scribe";

    private static final String LOG_RECEIVE = "logs/scribe/received/%s_%s.log";
    static Logger receiveLogger = Logger.getLogger("SendMessage");

    private Host myself;
    private Map<String, TopicManager> topicManagers;

    public ScribeProtocol(INetwork net) throws HandlerRegistrationException, NotificationDoesNotExistException, ProtocolDoesNotExist {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);
        myself = net.myHost();

        //Requests
        registerRequestHandler(DisseminateMessageRequest.REQUEST_ID, uponDisseminateMessageRequest);

        //Notification produced
        registerNotification(MessageDelivery.NOTIFICATION_ID, MessageDelivery.NOTIFICATION_NAME);

        //Declare Messages sent/received of the protocol
        registerMessageHandler(ScribeProtocolMessage.MSG_CODE, uponScribeProtocolMessage, ScribeProtocolMessage.serializer);

        try{
            FileHandler file = new FileHandler(String.format(LOG_RECEIVE, myself.getAddress(), myself.getPort()));
            receiveLogger.addHandler(file);
            SimpleFormatter formatter = new SimpleFormatter();
            file.setFormatter(formatter);

        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init(Properties properties) {
        topicManagers = new HashMap<>();
    }

    private ProtocolRequestHandler uponDisseminateMessageRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest r) {
            //Create Message
            DisseminateMessageRequest req = (DisseminateMessageRequest) r;
            String topic = req.getRequestType();
            switch (topic) {
                case "SUB":
                    handleSub(r);
                    break;
                case "UNSUB":
                    handleUnSub(r);
                    break;
                case "PUB":
                    handlePub(r);
                    break;
            }
        }

        private void handleSub(ProtocolRequest r) {
            DisseminateMessageRequest req = (DisseminateMessageRequest) r;
            String topic = req.getTopic();

            TopicManager manager = topicManagers.get(topic);

            //if we dont know the topic, send chord request
            if(manager == null){
                manager = new TopicManager(topic);
                topicManagers.put(topic, manager);

                BigInteger id = Hash.getHash(topic);
                ScribeProtocolMessage scribeMessage = new ScribeProtocolMessage(req.getRequestType(), req.getTopic(), req.getPayload());
                scribeMessage.setFrom(myself);
                RouteReqRequest chordRequest = new RouteReqRequest(ScribeProtocolMessage.serialize(scribeMessage), id);
                chordRequest.setDestination(Chord.PROTOCOL_ID);

                try {
                    sendRequest(chordRequest);
                } catch (DestinationProtocolDoesNotExist e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }

            manager.setSubscription(true);
        }

        private void handleUnSub(ProtocolRequest r) {
            DisseminateMessageRequest req = (DisseminateMessageRequest) r;
            String topic = req.getTopic();

            TopicManager manager = topicManagers.get(topic);

            if(manager != null){

                manager.setSubscription(false);

                Host root = manager.getRoot();

                if(manager.getChildrenSize() == 0){ //if there are no more children, we can forget the topic

                    topicManagers.remove(topic);

                    if( (root != null) && (!manager.isRoot(myself) )){
                        ScribeProtocolMessage msg = new ScribeProtocolMessage(req.getRequestType(),
                                req.getTopic(), req.getPayload());

                        msg.setFrom(myself);
                        sendMessage(msg, root);
                    }

                }
            }else{
                System.out.println("Attempt to unsub unknown topic!");
            }
        }

        private void handlePub(ProtocolRequest r) {
            DisseminateMessageRequest req = (DisseminateMessageRequest) r;
            String topic = req.getTopic();

            TopicManager manager = topicManagers.get(topic);

            if( manager != null){
                Set<Host> sendSet = manager.getChildren();
                Host root = manager.getRoot();

               /* if(!manager.isRoot(myself)){
                    sendSet.add(root);
                }*/

                for(Host h: sendSet){
                    ScribeProtocolMessage propagationMessage = new ScribeProtocolMessage(req.getRequestType(),
                            req.getTopic(), req.getPayload());

                    sendMessage(propagationMessage, h);
                }

                if(manager.amISubscribed()) {
                    MessageDelivery deliver = new MessageDelivery(topic, req.getPayload());
                    triggerNotification(deliver);
                }
            } /*else{
                BigInteger id = Hash.getHash(topic);
                ScribeProtocolMessage scribeMessage = new ScribeProtocolMessage(req.getRequestType(),
                        req.getTopic(), req.getPayload());
                RouteReqRequest chordRequest = new RouteReqRequest(ScribeProtocolMessage.serialize(scribeMessage), id);
                chordRequest.setDestination(Chord.PROTOCOL_ID);
                try {
                    sendRequest(chordRequest);
                } catch (DestinationProtocolDoesNotExist e) {
                    e.printStackTrace();
                    System.exit(1);
                }

            }*/
        }
    };

    public void deliverNotification(ProtocolNotification notification) {
        RouteDelNotification deliver = (RouteDelNotification) notification;
        byte[] message = deliver.getMessage();

        ScribeProtocolMessage scribeMessage = ScribeProtocolMessage.deserialize(message);
        String replyType = scribeMessage.getRequestType();
        receiveLogger.log(Level.INFO,  "SCRIBE: chordRply *** " + replyType + " *** ");
        switch (replyType) {
            case "SUB":
                handleSubChordReply(deliver);
                break;
            case "UNSUB":
                System.out.println("Nothing to check here.");
                //handleUnSubChordReply(deliver);
                break;
            case "PUB":
                handlePubChordReply(deliver);
                break;
        }

    }

    private void handleSubChordReply(RouteDelNotification deliver) {
        boolean isOwner = deliver.isOwner();
        Host nextNode = deliver.getNextNode(); // Node to which the message was propagated
        Host sender = deliver.getSender();
        byte[] message = deliver.getMessage();

        System.out.println(isOwner +" "+ sender +" "+ nextNode );

        ScribeProtocolMessage scribeMessage = ScribeProtocolMessage.deserialize(message);
        String topic = scribeMessage.getTopic();

        TopicManager manager = topicManagers.get(topic);
        boolean forwardSubscribe = false;

        if( manager == null ){
            manager = new TopicManager(topic);
            topicManagers.put(topic, manager);
            forwardSubscribe = true;
        }

        if(sender != null){
            manager.addChildren(sender);
            addNetworkPeer(sender);
        }

        if(isOwner) {
            manager.setRoot(myself);
        } else{

            if(nextNode == null){

                if(forwardSubscribe){

                    BigInteger id = Hash.getHash(topic);
                    ScribeProtocolMessage msg = new ScribeProtocolMessage(scribeMessage.getRequestType(),
                            scribeMessage.getTopic(), message);
                    RouteReqRequest chordRequest = new RouteReqRequest(ScribeProtocolMessage.serialize(scribeMessage), id);
                    chordRequest.setDestination(Chord.PROTOCOL_ID);
                    try {
                        sendRequest(chordRequest);
                    } catch (DestinationProtocolDoesNotExist e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }

            }else{
                Host lastRoot = manager.setRoot(nextNode);
                if(lastRoot != null) removeNetworkPeer(lastRoot);
                addNetworkPeer(nextNode);

            }
        }

    }

    /*
    //TODO This will never be called!
    private void handleUnSubChordReply(RouteDelNotification deliver) {
        Host nextNode = deliver.getNextNode(); // Node to with he message was propagated
        byte[] message = deliver.getMessage();

        ScribeProtocolMessage scribeMessage = ScribeProtocolMessage.deserialize(message);
        String topic = scribeMessage.getTopic();

        if (topicLinks.get(topic).remove(nextNode))
            if (topicLinks.get(topic).size() == 0)
                topicLinks.remove(topic);

    }
    */

    private void handlePubChordReply(RouteDelNotification deliver) {
        boolean isOwner = deliver.isOwner();
        Host previousNode = deliver.getSender();
        Host nextNode = deliver.getNextNode(); // Node to with he message was propagated
        byte[] message = deliver.getMessage();

        ScribeProtocolMessage scribeMessage = ScribeProtocolMessage.deserialize(message);
        String topic = scribeMessage.getTopic();

        TopicManager manager = topicManagers.get(topic);
        // boolean sendChord = false;

        /*if( manager == null ){
            manager = new TopicManager(topic);
            topicManagers.put(topic, manager);
            sendChord = true;
        }*/

        if(isOwner){
            manager = new TopicManager(topic);
            topicManagers.put(topic, manager);
            manager.setRoot(myself);
        }else{
            if(nextNode == null){
                // receiving a chord message

                /*manager.addChildren(previousNode);
                addNetworkPeer(previousNode);*/

                if(manager == null){  //if i didnt know about the topic, send chord request
                    BigInteger id = Hash.getHash(topic);
                    RouteReqRequest chordRequest = new RouteReqRequest(ScribeProtocolMessage.serialize(scribeMessage), id);
                    chordRequest.setDestination(Chord.PROTOCOL_ID);
                    try {
                        sendRequest(chordRequest);
                    } catch (DestinationProtocolDoesNotExist e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }else{
                    Set<Host> sendSet = manager.getChildren();
                    Host root = manager.getRoot();
                    if (previousNode != null) {
                        sendSet.remove(previousNode);
                    }

                    if(!manager.isRoot(myself)){
                        sendSet.add(root);
                    }

                    scribeMessage.setFrom(myself);

                    for(Host h: sendSet){
                        sendMessage(scribeMessage, h);
                    }

                    if(manager.amISubscribed()) {
                        MessageDelivery notifyPubSub = new MessageDelivery(topic, scribeMessage.getMessage());
                        triggerNotification(notifyPubSub);
                    }
                }
            }/*else{
                // chord message sended to
                Host lastRoot = manager.setRoot(nextNode);
                if(lastRoot != null) removeNetworkPeer(lastRoot);
                addNetworkPeer(nextNode);
            }*/
        }

    }


    private ProtocolMessageHandler uponScribeProtocolMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage protocolMessage) {
            ScribeProtocolMessage msg = (ScribeProtocolMessage) protocolMessage;
            String topic = msg.getRequestType();
            receiveLogger.log(Level.INFO,  "SCRIBE: uponScribe *** " + topic + " *** ");

            switch (topic) {
                case "SUB":
                    System.out.println("Nothing to check here.");
                    //handleSub(protocolMessage);
                    break;
                case "UNSUB":
                    handleUnSub(protocolMessage);
                    break;
                case "PUB":
                    handlePub(protocolMessage);
                    break;
            }
        }

        /*
        //TODO It will never be used
        private void handleSub(ProtocolMessage r) {
            ScribeProtocolMessage req = (ScribeProtocolMessage) r;
            String topic = req.getTopic();

            Set<Host> nextNodesToReceiveTopic = topicLinks.get(topic);
            if (nextNodesToReceiveTopic != null) { // if I have information about the topic simply add myself to my propagation list

                addNodeToNodesLinks(myself, topic); // TODO: nao sei se realmente se faz algo uma vez que nunca cai neste if

            } else {
                BigInteger id = Hash.getHash(topic);
                ScribeProtocolMessage scribeMessage = new ScribeProtocolMessage(req.getRequestType(), req.getTopic(), req.getMessage());
                RouteReqRequest chordRequest = new RouteReqRequest(ScribeProtocolMessage.serialize(scribeMessage), id);
                try {
                    sendRequest(chordRequest);
                } catch (DestinationProtocolDoesNotExist e) {
                    e.printStackTrace();
                    System.exit(1);
                }

            }
        }
        */

        private void handleUnSub(ProtocolMessage r) {
            ScribeProtocolMessage req = (ScribeProtocolMessage) r;
            String topic = req.getTopic();

            TopicManager manager = topicManagers.get(topic);

            if(manager != null){

                Host root = manager.getRoot();
                Host sender = req.getFrom();
                manager.removeChildren(sender);
                removeNetworkPeer(sender);

                if(manager.getChildrenSize() == 0){ //if we remove the sender and there are no more children, we can unsub

                    topicManagers.remove(topic);

                    if( (root != null) && (!manager.isRoot(myself) )){
                        ScribeProtocolMessage msg = new ScribeProtocolMessage(req.getRequestType(),
                                req.getTopic(), null);

                        msg.setFrom(myself);
                        sendMessage(msg, root);
                    }

                }
            }else{
                System.out.println("ERROR");
            }
        }

        private void handlePub(ProtocolMessage r) {
            ScribeProtocolMessage req = (ScribeProtocolMessage) r;
            String topic = req.getTopic();
            Host sender = req.getFrom();

            TopicManager manager = topicManagers.get(topic);

            if(manager != null){

                Set<Host> sendSet = manager.getChildren();
                sendSet.remove(sender);

                req.setFrom(myself);

                for(Host h: sendSet){
                    sendMessage(req, h);
                }

                if(manager.amISubscribed()) {
                    MessageDelivery notifyPubSub = new MessageDelivery(topic, req.getMessage());
                    triggerNotification(notifyPubSub);
                }

            }
        }
    };

    private void sendChordRequest(){

    }

}