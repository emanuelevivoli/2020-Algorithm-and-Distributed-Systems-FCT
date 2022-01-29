package babel.demos.protocols.chord;

import babel.demos.protocols.chord.messages.*;
import babel.demos.protocols.chord.notifications.FindRootNotification;
import babel.demos.protocols.chord.notifications.RouteDelNotification;
import babel.demos.protocols.chord.requests.FindRootRequest;
import babel.demos.protocols.chord.requests.RouteReqRequest;
import babel.demos.protocols.chord.timers.FixFingersProtocolTimer;
import babel.demos.protocols.chord.timers.StabilizeProtocolTimer;
import babel.demos.protocols.chord.utils.ChordNode;
import babel.demos.utils.Hash;
import babel.demos.utils.ID;
import babel.exceptions.HandlerRegistrationException;
import babel.exceptions.NotificationDoesNotExistException;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolRequest;
import babel.timer.ProtocolTimer;
import network.Host;
import network.INetwork;
import network.INodeListener;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
// import java.util.Random;

public class Chord extends GenericProtocol implements INodeListener {
    public final static int JOIN_CODE = -1; // TODO: change in short everywhere
    public final static int RESP_CODE = -2;
    public final static short STABILIZE_RATE_MS = 1000;
    public final static short FIX_FINGERS_RATE_MS = 1000;
//    public final static short CHECK_PREDECESSOR_RATE_MS = 1000;
    public final static short PROTOCOL_ID = 10;
    public final static String PROTOCOL_NAME = "Chord";

    private static final String LOG_SEND = "logs/chord/sent/%s_%s.log";
    static Logger sendLogger = Logger.getLogger("SendMessage");

    private static final String LOG_LOAD_BALANCE = "logs/chord/loadBalancing/%s_%s.log";
    static Logger loadBalanceLogger = Logger.getLogger("LoadBalance");

    private static short NUMBER_OF_BITS;
    private ChordNode[] fingers;
    private ChordNode myself, successor, predecessor;
    private Host myHost;
    private int nextFingerToFix;

    public Chord(INetwork net/*, BigInteger nodeId*/) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);
        myHost = net.myHost();

        //Requests
        registerRequestHandler(RouteReqRequest.REQUEST_ID, uponRouteRequest);
        // find root for a topic
        registerRequestHandler(FindRootRequest.REQUEST_ID, uponFindRootRequest);

        //Declare Messages sent/received of the protocol
        // used for join and for fix finger
        registerMessageHandler(JoinProtocolMessage.MSG_CODE, findSuccessor, JoinProtocolMessage.serializer);
        registerMessageHandler(JoinReplyProtocolMessage.MSG_CODE, findSuccessorReply, JoinReplyProtocolMessage.serializer);
        // used for stabilize
        registerMessageHandler(PredecessorRequestProtocolMessage.MSG_CODE, uponPredecessorRequestMessage, PredecessorRequestProtocolMessage.serializer);
        registerMessageHandler(PredecessorReplyProtocolMessage.MSG_CODE, uponPredecessorReplyMessage, PredecessorReplyProtocolMessage.serializer);
        // use for notify
        registerMessageHandler(NotifyProtocolMessage.MSG_CODE, uponNotifyProtocolMessage, NotifyProtocolMessage.serializer);
        // route message between Chord
        registerMessageHandler(RouteProtocolMessage.MSG_CODE, uponRouteMessage, RouteProtocolMessage.serializer);


        //Shuffle timer
        registerTimerHandler(StabilizeProtocolTimer.TimerCode, uponStabilizeTimerProtocolTimer);
        registerTimerHandler(FixFingersProtocolTimer.TimerCode, uponFixFingersProtocolTimer);

        //Notifications Produced
        registerNotification(RouteDelNotification.NOTIFICATION_ID, RouteDelNotification.NOTIFICATION_NAME);
        //Notifications for find the root
        registerNotification(FindRootNotification.NOTIFICATION_ID, FindRootNotification.NOTIFICATION_NAME);



        try{
            FileHandler file = new FileHandler(String.format(LOG_LOAD_BALANCE, myHost.getAddress(), myHost.getPort()));
            loadBalanceLogger.addHandler(file);
            SimpleFormatter formatter = new SimpleFormatter();
            file.setFormatter(formatter);

            FileHandler file2 = new FileHandler(String.format(LOG_SEND, myHost.getAddress(), myHost.getPort()));
            //sendLogger.addHandler(file2);
            file2.setFormatter(formatter);

        }catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void init(Properties props) {
        registerNodeListener(this);
        //Setup configuration of the protocol
        NUMBER_OF_BITS = Short.parseShort(props.getProperty("numberOfBits", "256"));
        BigInteger nodeId = Hash.getHash(myHost.toString());
        myself = new ChordNode(nodeId, myHost);
        predecessor = new ChordNode(null, null);
        successor = myself;
        nextFingerToFix = 0;

        // Setup protocol timer (first, period)
        setupPeriodicTimer(new StabilizeProtocolTimer(), STABILIZE_RATE_MS, STABILIZE_RATE_MS);
        setupPeriodicTimer(new FixFingersProtocolTimer(), FIX_FINGERS_RATE_MS, FIX_FINGERS_RATE_MS);

        fingers = new ChordNode[NUMBER_OF_BITS];

        if (props.containsKey("Contact")) {
            try {
                String contact = props.getProperty("Contact");
                String[] hostElements = contact.split(":");
                Host contactNodeAddress = new Host(InetAddress.getByName(hostElements[0]), Short.parseShort(hostElements[1]));
                BigInteger contactNodeId = Hash.getHash(contact);
                ChordNode contactNode = new ChordNode(contactNodeId, contactNodeAddress);
                if (!contactNode.equals(myself)) {
                    join(contactNode);
                }

            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }

    }

    private void join(ChordNode contactNode) {
        ProtocolMessage msg = new JoinProtocolMessage(myself, -1);
        addNetworkPeer(contactNode.getNodeAddress());
        sendMessage(msg, contactNode.getNodeAddress());
        //sendLogger.log(Level.INFO,  "JoinProtocol");
    }

    // receiving a join message or a fix_finger find successor
    private final ProtocolMessageHandler findSuccessor = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            ChordNode newNode = ((JoinProtocolMessage) msg).getNewNode();
            int next = ((JoinProtocolMessage) msg).getNext();
            find_successor(newNode, next);
        }
    };

    private void find_successor(ChordNode nodeWaitingResponse, int next) {
        BigInteger newNodeId = nodeWaitingResponse.getNodeId();
        Host newNodeAddress = nodeWaitingResponse.getNodeAddress();

        if (next == RESP_CODE){
            System.out.println("ola");
        }

        if (next == JOIN_CODE) {
            if (successor.equals(myself)) {
                ProtocolMessage join_reply = new JoinReplyProtocolMessage(myself, next);
                addNetworkPeer(newNodeAddress);
                sendMessage(join_reply, newNodeAddress);
                //sendLogger.log(Level.INFO,  "JoinReplyProtocol");
                changeSuccessor(nodeWaitingResponse, true);
            } else if (ChordNode.intervalOpenClose(myself, successor, nodeWaitingResponse)) {
                ProtocolMessage join_reply = new JoinReplyProtocolMessage(successor, next);
                addNetworkPeer(newNodeAddress);
                sendMessage(join_reply, newNodeAddress);
                //sendLogger.log(Level.INFO,  "JoinReplyProtocol");
            } else {
                ChordNode closes_preceding = closest_preceding_node(nodeWaitingResponse);
                ProtocolMessage forward_join = new JoinProtocolMessage(nodeWaitingResponse, next);
                addNetworkPeer(closes_preceding.getNodeAddress());
                sendMessage(forward_join, closes_preceding.getNodeAddress());
                //sendLogger.log(Level.INFO,  "JoinProtocol");
            }

        } else
            // if i'm managing fix-fingers messages or find-responsable messages
            {
            if (ChordNode.uniIntervalOpenClose(myself, successor, nodeWaitingResponse)) {
                /*System.err.println("*********************************");
                System.err.println("Predecessor: " + (predecessor.getNodeAddress() == null ?
                        "null" : predecessor.getNodeAddress().toString()));
                System.err.println(predecessor.getNodeId() == null ? "null" : predecessor.getNodeId());
                System.err.println("Successor: " + (successor.getNodeAddress() == null ?
                        "null" : successor.getNodeAddress().toString()));
                System.err.println(successor.getNodeId() == null ? "null" : successor.getNodeId());
                System.err.println("**********************************");*/
                ProtocolMessage join_reply = new JoinReplyProtocolMessage(successor, next, newNodeId);
                addNetworkPeer(newNodeAddress);
                //sendLogger.log(Level.INFO,  "JoinReplyProtocol");
                sendMessage(join_reply, newNodeAddress);
            } else {
                ChordNode closes_preceding = closest_preceding_node(nodeWaitingResponse);
                ProtocolMessage forward_join = new JoinProtocolMessage(nodeWaitingResponse, next);
                addNetworkPeer(closes_preceding.getNodeAddress());
                //sendLogger.log(Level.INFO,  "JoinProtocol");
                sendMessage(forward_join, closes_preceding.getNodeAddress());
            }
        }
    }

    private ChordNode closest_preceding_node(ChordNode nodeToFind) {
        for (int i = NUMBER_OF_BITS; i > 1; i--) {
            ChordNode currFinger = fingers[i - 1];
            if (currFinger != null && ChordNode.intervalOpenOpen(myself, nodeToFind, currFinger))
                return currFinger;
        }
        return myself;
    }

    private final void changeSuccessor(ChordNode suc, boolean changeFinger) {
        successor = suc;
        fingers[0] = suc;
        if (changeFinger) {
            BigInteger succNext = suc.getNodeId();
            int i = 1;
            BigInteger next = ID.getID(NUMBER_OF_BITS, i, myself.getNodeId());
            do {

                fingers[i] = suc;
                next = ID.getID(NUMBER_OF_BITS, ++i, myself.getNodeId());

            } while (next.compareTo(succNext) <= 0 && i < NUMBER_OF_BITS);
        }
        System.err.println("Successor: " + (successor.getNodeAddress() == null ?
                "null" : successor.getNodeAddress().toString()));
    }

    // receiving a join message reply or a fix_finger find successor reply
    private final ProtocolMessageHandler findSuccessorReply = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            ChordNode suc = ((JoinReplyProtocolMessage) msg).getNewNode();
            int next = ((JoinReplyProtocolMessage) msg).getNext();
            BigInteger id = ((JoinReplyProtocolMessage) msg).getChordId();
            join_fix_receive(suc, next, id);
        }
    };

    private final void join_fix_receive(ChordNode suc, int next, BigInteger id) {
        if (next == JOIN_CODE) {
            changeSuccessor(suc, true);
        } else if (next == RESP_CODE) {
            // TODO: reply to scribe
            System.out.println("[CHORD] - join_fix_receive w/ RESP_CODE | "+id);
            FindRootNotification rootDeliver = new FindRootNotification(suc.getNodeAddress(), id);
            triggerNotification(rootDeliver);

        }  else {
            fingers[next] = suc;
        }
    }


    /*
     **************************
     **       TIMERS         **
     **************************
     */

    // stabilize timer
    private ProtocolTimerHandler uponStabilizeTimerProtocolTimer = new ProtocolTimerHandler() {
        @Override
        public void uponTimer(ProtocolTimer protocolTimer) {
            stabilize();
        }
    };

    // stabilize making request
    private void stabilize() {
        if (successor.equals(myself)) {  // only the first node has successor == myself
            // System.out.println("[CHORD - ERROR] Successor = myself in Stabilize.");
            return;
        }
        ProtocolMessage request = new PredecessorRequestProtocolMessage(successor);
        addNetworkPeer(successor.getNodeAddress());
        //sendLogger.log(Level.INFO,  "PredecessorRequestProtocol");
        sendMessage(request, successor.getNodeAddress());
    }

    // stabilize getting request - making reply
    private ProtocolMessageHandler uponPredecessorRequestMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            // System.out.println("[CHORD] stabilize request");
            //If i dont have predecessor, i send null
            ProtocolMessage reply = new PredecessorReplyProtocolMessage(predecessor);
            addNetworkPeer(msg.getFrom());
            sendMessage(reply, msg.getFrom());
            //sendLogger.log(Level.INFO,  "PredecessorReplyProtocol");
        }
    };

    // stabilize getting reply - notify call for successor
    private ProtocolMessageHandler uponPredecessorReplyMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            // System.out.println("[CHORD] stabilize reply");
            PredecessorReplyProtocolMessage reply = (PredecessorReplyProtocolMessage) msg;
            ChordNode successor_predecessor = reply.getNewNode();

            // Assumes that if predecessor of successor is null, the successor returns itself
            if (ChordNode.intervalOpenOpen(myself, successor, successor_predecessor)) {
                if (!myself.getNodeAddress().toString().equals(successor_predecessor.getNodeAddress().toString()))
                    changeSuccessor(successor_predecessor, true);

            }

            ProtocolMessage notify = new NotifyProtocolMessage(myself);
            addNetworkPeer(successor.getNodeAddress());
            //sendLogger.log(Level.INFO,  "NotifyProtocol");
            sendMessage(notify, successor.getNodeAddress());

        }
    };

    // notify getting from predecessor
    private ProtocolMessageHandler uponNotifyProtocolMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            NotifyProtocolMessage reply = (NotifyProtocolMessage) msg;
            ChordNode sender = reply.getNewNode();
            if (predecessor.getNodeId() == null || ChordNode.uniIntervalOpenOpen(predecessor, myself, sender)) {
                predecessor = sender;
                System.err.println("Predecessor: " + (predecessor.getNodeAddress() == null ?
                        "null" : predecessor.getNodeAddress().toString()));

            }
        }
    };

    // fix_fingers timer - making request with find_successor function (message join)
    private ProtocolTimerHandler uponFixFingersProtocolTimer = new ProtocolTimerHandler() {
        @Override
        public void uponTimer(ProtocolTimer protocolTimer) {
            if (successor.equals(myself)) {
                return;
            }
            nextFingerToFix++;
            if (nextFingerToFix >= NUMBER_OF_BITS)
                nextFingerToFix = 1;

            ChordNode nodeToFind = new ChordNode(ID.getID(NUMBER_OF_BITS, nextFingerToFix, myself.getNodeId()), myself.getNodeAddress());
            find_successor(nodeToFind, nextFingerToFix);
        }
    };

    static Set<BigInteger> topicsSet = new HashSet<>();

    // notify getting from predecessor
    private ProtocolMessageHandler uponRouteMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            RouteProtocolMessage routeMessage = (RouteProtocolMessage) msg;

            ChordNode sender = routeMessage.getNewNode();
            BigInteger id = sender.getNodeId();

            byte[] msgContent = routeMessage.getMessage();
            ChordNode node = new ChordNode(id, myself.getNodeAddress());

            if (ChordNode.uniIntervalOpenClose(predecessor, myself, node)) {
                RouteDelNotification deliver = new RouteDelNotification(true, null,
                        sender.getNodeAddress(), msgContent);
                triggerNotification(deliver);
                if(!topicsSet.contains(id)){
                    loadBalanceLogger.log(Level.INFO,  id.toString());
                    topicsSet.add(id);
                }

            } else {
                RouteDelNotification deliver = new RouteDelNotification(false, null,
                        sender.getNodeAddress(), msgContent);
                triggerNotification(deliver);
            }

        }
    };

    /*
     **************************
     **       REQUESTS       **
     **************************
     */
    private ProtocolRequestHandler uponRouteRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest request) {
            RouteReqRequest subRequest = (RouteReqRequest) request;

            byte[] msg = subRequest.getMessage();
            BigInteger id = subRequest.getChordId();

            routeMessage(myself, msg, id);
        }
    };

    private ProtocolRequestHandler uponFindRootRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest request) {
            FindRootRequest subRequest = (FindRootRequest) request;

            BigInteger id = subRequest.getChordId();
            ChordNode newNode = new ChordNode(id, myHost);
            System.out.println("[CHORD] - uponFindRootRequest | "+id);
            find_successor(newNode, RESP_CODE);
        }
    };

    private void routeMessage(ChordNode sender, byte[] msg, BigInteger id) {
        ChordNode node = new ChordNode(id, myself.getNodeAddress());

        BigInteger nodeId = node.getNodeId();
        Host nodeAddress = node.getNodeAddress();

        ChordNode receiver;
        RouteProtocolMessage routeRequest;

        // IMPORTANT: id in (n, successor)
        if (ChordNode.uniIntervalOpenClose(predecessor, myself, node)) {
            RouteDelNotification deliver = new RouteDelNotification(true,  null,
                    null, msg);
            triggerNotification(deliver);
            if(!topicsSet.contains(id)){
                loadBalanceLogger.log(Level.INFO,  id.toString());
                topicsSet.add(id);
            }

        } else {

            if (ChordNode.uniIntervalOpenClose(myself, successor, node)) {
                receiver = successor;
            } else {
                receiver = closest_preceding_node(node);
            }

            routeRequest = new RouteProtocolMessage(myself, msg);
            addNetworkPeer(receiver.getNodeAddress());
            sendMessage(routeRequest, receiver.getNodeAddress());
            //sendLogger.log(Level.INFO,  "*************RouteProtocol ***********");

            RouteDelNotification deliver = new RouteDelNotification(false, receiver.getNodeAddress(),
                    null, msg);
            triggerNotification(deliver);

        }

    }

    // equal to check_predecessor
    @Override
    public void nodeDown(Host host) {
        System.out.println("[CHORD] node down ".concat(host.toString()));
        Host predecessorAddress = predecessor.getNodeAddress();
        Host successorAddress = successor.getNodeAddress();

        ChordNode newSuccessor = failuresReplace(host);

        if (predecessorAddress == null) {
            return;
        }
        if (predecessorAddress.equals(host)) {
            predecessor = new ChordNode(null, null);
            System.err.println("Predecessor: " + (predecessor.getNodeAddress() == null ?
                    "null" : predecessor.getNodeAddress().toString()));
        } else if (successorAddress.equals(host)) {
            if (newSuccessor != null)
                changeSuccessor(newSuccessor, false);
            else
                changeSuccessor(myself, false);
        }
        removeNetworkPeer(host);

    }

    private ChordNode failuresReplace(Host host) {
        BigInteger nodeId = Hash.getHash(host.toString());

        ChordNode node = new ChordNode(nodeId, host);
        ChordNode newSuccessor = null;
        List<Integer> replaceList = new LinkedList<Integer>();
        boolean passed = false;

        for (int i = 1; i < NUMBER_OF_BITS; i++) {
            ChordNode currNode = fingers[i];

            if (currNode == null) break;

            if (node.equals(currNode)) {
                passed = true;
                replaceList.add(i);
            } else if (passed) {
                newSuccessor = currNode;
                break;
            }
        }

        if (passed) {
            for (Integer index : replaceList) {
                fingers[index] = newSuccessor;
            }
        }
        return newSuccessor;
    }


    @Override
    public void nodeUp(Host host) {
        System.out.println("[CHORD] node up ".concat(host.toString()));
    }

    @Override
    public void nodeConnectionReestablished(Host host) {
        System.out.println("[CHORD] node reconnect ".concat(host.toString()));
    }
}