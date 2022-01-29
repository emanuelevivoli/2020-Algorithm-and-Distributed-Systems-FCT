package babel.demos.protocols.hyParView;

import babel.demos.protocols.hyParView.notifications.NodeMembershipNotification;
import babel.demos.protocols.hyParView.messages.*;
import babel.demos.protocols.hyParView.requests.GetPeerNeighboursReply;
import babel.demos.protocols.hyParView.requests.GetPeerNeighboursRequest;
import babel.demos.protocols.hyParView.timers.ShuffleProtocolTimer;
import babel.demos.protocols.hyParView.timers.TryConnectionProtocolTimer;
import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.exceptions.HandlerRegistrationException;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class HyParView extends GenericProtocol implements INodeListener {

    //Numeric identifier of the protocol
    public final static short PROTOCOL_ID = 1;
    public final static String PROTOCOL_NAME = "HyparView";

    static Logger sendMessagesLogger = Logger.getLogger("SendHyparviewMessages");
    private static final String LOG_SEND_FILE = "logs/hyparview/send%s_%s.log";

    private static short ARWL, PRWL;
    private static short PASSIVE_VIEW_MAX_SIZE, ACTIVE_VIEW_MAX_SIZE;

    private Host myself, contactNode, newNode;
    private List<Host> activeView, passiveView, lastShuffle;

    //Shuffle Protocol
    private static short SHUFFLE_TTL;
    private static short SHUFFLE_MS;
    private static short SHUFFLE_KA;
    private static short SHUFFLE_KP;
    private static short TRY_CONNECTION_MS;

    public HyParView(INetwork net) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);
        myself = net.myHost();

        //Requests
        registerRequestHandler(GetPeerNeighboursRequest.REQUEST_ID, uponGetPeerNeighboursRequest);

        //Declare Messages sent/received of the protocol
        registerMessageHandler(JoinProtocolMessage.MSG_CODE, uponJoinMessage, JoinProtocolMessage.serializer);
        registerMessageHandler(ForwardJoinProtocolMessage.MSG_CODE, uponForwardJoinMessage, ForwardJoinProtocolMessage.serializer);
        registerMessageHandler(DisconnectProtocolMessage.MSG_CODE, uponDisconnectMessage, DisconnectProtocolMessage.serializer);
        registerMessageHandler(NeighbourRequestProtocolMessage.MSG_CODE, uponNeighbourRequestMessage, NeighbourRequestProtocolMessage.serializer);
        registerMessageHandler(NeighbourReplyProtocolMessage.MSG_CODE, uponNeighbourReplyMessage, NeighbourReplyProtocolMessage.serializer);

        //Passive view protocol messages
        registerMessageHandler(ShuffleRequestProtocolMessage.MSG_CODE, uponShuffleRequestMessage, ShuffleRequestProtocolMessage.serializer);
        registerMessageHandler(ShuffleReplyProtocolMessage.MSG_CODE, uponShuffleReplyMessage, ShuffleReplyProtocolMessage.serializer);

        //Shuffle timer
        registerTimerHandler(ShuffleProtocolTimer.TimerCode, uponShuffleProtocolTimer);
        registerTimerHandler(TryConnectionProtocolTimer.TimerCode, uponConnectionTryTimer);

        //Notifications Produced
        registerNotification(NodeMembershipNotification.NOTIFICATION_ID, NodeMembershipNotification.NOTIFICATION_NAME);

        try {
            SimpleFormatter formatter = new SimpleFormatter();
            FileHandler sendfh = new FileHandler(String.format(LOG_SEND_FILE, myself.getAddress(), myself.getPort()));
            sendMessagesLogger.addHandler(sendfh);
            sendfh.setFormatter(formatter);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void init(Properties props) {
        registerNodeListener(this);

        //Setup configuration of the protocol
        ARWL = Short.parseShort(props.getProperty("active_random_walk_length", "6"));
        PRWL = Short.parseShort(props.getProperty("passive_random_walk_length", "3"));
        ACTIVE_VIEW_MAX_SIZE = Short.parseShort(props.getProperty("active_view_length", "5"));
        PASSIVE_VIEW_MAX_SIZE = Short.parseShort(props.getProperty("passive_view_length", "30"));

        //Passive view Shuffle
        SHUFFLE_TTL = Short.parseShort(props.getProperty("shuffle_ttl", "5"));
        SHUFFLE_MS = Short.parseShort(props.getProperty("shuffle_ms", "5000"));
        SHUFFLE_KA = Short.parseShort(props.getProperty("shuffle_ka", "3"));
        SHUFFLE_KP = Short.parseShort(props.getProperty("shuffle_kp", "4"));
        TRY_CONNECTION_MS = Short.parseShort(props.getProperty("try_connection_ms", "2000"));

        this.activeView = new CopyOnWriteArrayList<>();
        this.passiveView = new CopyOnWriteArrayList<>();
        this.newNode = myself;


        if (props.containsKey("Contact")) {
            try {
                String[] hostElems = props.getProperty("Contact").split(":");
                contactNode = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));

                //add contacted node to the active view and open a link to it
                addNetworkPeer(contactNode);
                addNodeActiveView(contactNode);

                //request entry node's
                JoinProtocolMessage gm = new JoinProtocolMessage(myself);
                sendMessagesLogger.log(Level.INFO, "joinmessage");
                sendMessage(gm, contactNode);

            } catch (UnknownHostException e) {
                System.err.println("Invalid contact on configuration: '" + props.getProperty("Contact"));
            }
        }

        //Setup protocol timer (first, period)
        setupPeriodicTimer(new ShuffleProtocolTimer(), SHUFFLE_MS, SHUFFLE_MS);
    }

    private List<Host> getActiveView() {
        return activeView;
    }

    private void dropRandomElementFromPassiveView() {
        Random rnd = new Random();
        int index = rnd.nextInt(passiveView.size());
        passiveView.remove(index);
    }

    private void dropRandomElementFromActiveView() {
        Random rnd = new Random();
        int index = rnd.nextInt(activeView.size());

        // delete node from active view
        Host selectedNode = activeView.remove(index);
        DisconnectProtocolMessage dm = new DisconnectProtocolMessage(myself);
        sendMessagesLogger.log(Level.INFO, "disconnectmessage");
        sendMessage(dm, selectedNode);

        addNodePassiveView(selectedNode);
        removeNetworkPeer(selectedNode);// code to delete TCP.
    }

    private void addNodePassiveView(Host node) {
        if (!node.equals(myself) && !passiveView.contains(node)) {
            if (passiveView.size() == PASSIVE_VIEW_MAX_SIZE) {
                dropRandomElementFromPassiveView();
            }
            passiveView.add(node);
        }
    }

    private void addNodeActiveView(Host node) {
        if (!node.equals(myself) && !activeView.contains(node)) {
            if (isActiveViewFull()) {
                dropRandomElementFromActiveView();
            }
            addNetworkPeer(node);

            passiveView.remove(node);
            activeView.add(node);
        }
    }

    private boolean isActiveViewFull() {
        return activeView.size() == ACTIVE_VIEW_MAX_SIZE;
    }


    private final ProtocolMessageHandler uponJoinMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            Host newNode = ((JoinProtocolMessage) msg).getNewNode();

            addNodeActiveView(newNode);

            List<Host> activeView = getActiveView();
            for (Host node : activeView) {
                if (node != newNode) {
                    ForwardJoinProtocolMessage fjm = new ForwardJoinProtocolMessage(newNode, ARWL);
                    fjm.setFrom(myself);
                    sendMessagesLogger.log(Level.INFO, "fowardjoinnmessage");
                    sendMessage(fjm, node);
                }
            }
        }
    };

    private final ProtocolMessageHandler uponForwardJoinMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {

            Host newNode = ((ForwardJoinProtocolMessage) msg).getNewNode();
            Host sender = ((ForwardJoinProtocolMessage) msg).getFrom();
            short timeToLive = ((ForwardJoinProtocolMessage) msg).getActiveRandomWalk();

            if (timeToLive == 0 || activeView.size() == 1) {
                addNodeActiveView(newNode);
                NeighbourRequestProtocolMessage nm = new NeighbourRequestProtocolMessage(myself);
                sendMessagesLogger.log(Level.INFO, "neighbourrequestmessage");
                sendMessage(nm, newNode);
            } else {
                if (timeToLive == PRWL) {
                    addNodePassiveView(newNode);
                }
                //select random node from active view
                Random rnd = new Random();
                int index = rnd.nextInt(activeView.size());
                Host receiver = activeView.get(index);
                while (receiver.equals(sender) || receiver.equals(newNode)) {
                    index = rnd.nextInt(activeView.size());
                    receiver = activeView.get(index);
                }
                timeToLive--;
                ForwardJoinProtocolMessage fjm = new ForwardJoinProtocolMessage(newNode, timeToLive);
                fjm.setFrom(myself);
                sendMessagesLogger.log(Level.INFO, "fowardjoinmessage");
                sendMessage(fjm, receiver);
            }
        }
    };


    private final ProtocolMessageHandler uponDisconnectMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            Host peer = ((DisconnectProtocolMessage) msg).getPeer();
            if (activeView.contains(peer)) {
                activeView.remove(peer);
                removeNetworkPeer(peer);
                addNodeFromPassiveViewToActiveView();
            }
        }
    };

    private void addNodeFromPassiveViewToActiveView() {
        if (passiveView.size() > 0) {
            Random rng = new Random();
            int index = rng.nextInt(passiveView.size());
            Host node = passiveView.remove(index);
            NeighbourRequestProtocolMessage nm = new NeighbourRequestProtocolMessage(myself);
            sendMessagesLogger.log(Level.INFO, "neighbourrequestmessage");
            sendMessageSideChannel(nm, node);

        }
    }

    private final ProtocolMessageHandler uponNeighbourRequestMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            Host sender = ((NeighbourRequestProtocolMessage) msg).getSender();
            if (passiveView.contains(sender)) {
                passiveView.remove(sender);
            }

            // Accept neighbour request
            addNodeActiveView(sender);
            NeighbourReplyProtocolMessage nrm = new NeighbourReplyProtocolMessage(myself, true);
            sendMessagesLogger.log(Level.INFO, "neighbourreplymessage");
            sendMessage(nrm, sender);
        }
    };

    private final ProtocolMessageHandler uponNeighbourReplyMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage msg) {
            NeighbourReplyProtocolMessage reply = (NeighbourReplyProtocolMessage) msg;
            Host sender = reply.getSender();
            if (reply.getAccept()) {
                addNodeActiveView(sender);
                Host toRemove = ((NeighbourReplyProtocolMessage) msg).getSender();
                UUID timerId = hostToConnectTimer.remove(toRemove.hashCode());
                cancelTimer(timerId);
                hostToConnected.remove(toRemove);
            }
        }
    };

    private ProtocolRequestHandler uponGetPeerNeighboursRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest request) {
            GetPeerNeighboursRequest req = (GetPeerNeighboursRequest) request;

            //Compute answer
            List<Host> toSend = new ArrayList<>(activeView);

            if (req.getSenderHost() != null)
                toSend.remove(req.getSenderHost());

            //send notification
            GetPeerNeighboursReply reply = new GetPeerNeighboursReply(req.getIdentifier(), toSend, req.getRequestType());
            reply.invertDestination(req);
            try {
                sendReply(reply);
            } catch (DestinationProtocolDoesNotExist e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    };

    private ProtocolTimerHandler uponShuffleProtocolTimer = new ProtocolTimerHandler() {
        @Override
        public void uponTimer(ProtocolTimer protocolTimer) {


            // Only applies if the node is connected to someone
            if (activeView.size() == 0 || passiveView.size() == 0) {
                // System.out.println("[SHUFF - TIM] Active: " + activeView.toString());
                // System.out.println("[SHUFF - TIM] Passive: " + passiveView.toString() + "\n");
                return;
            }

            /* Create sample list,
             * select random node from active view
             * create protocol message, setFrom(myself)
             * send shuffle request*/
            List<Host> sample = populateSample(new ArrayList<Host>());

            // Select random node from active view
            Host receiver = getNonMatchingRandomNodeFromActiveView(myself);

            // Send Shuffle Request to selected node using TCP connection
            ShuffleRequestProtocolMessage srm = new ShuffleRequestProtocolMessage(myself, sample, SHUFFLE_TTL);
            srm.setFrom(myself);
            sendMessagesLogger.log(Level.INFO, "shufflerequestmessage");
            sendMessage(srm, receiver);

            sample.remove(myself);
            lastShuffle = filterSample(sample, activeView);

            //System.out.println("[SHUFF - TIM] Active: " + activeView.toString());
            //System.out.println("[SHUFF - TIM] Passive: " + passiveView.toString() + "\n");
        }
    };

    private final ProtocolMessageHandler uponShuffleRequestMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage protocolMessage) {

            ShuffleRequestProtocolMessage msg = (ShuffleRequestProtocolMessage) protocolMessage;
            Host owner = msg.getOwner();
            List<Host> peerSample = msg.getSample();
            short timeToLive = msg.getPassiveRandomWalk();
            Host sender = msg.getFrom();


            timeToLive--;

            if (timeToLive > 0 && activeView.size() > 1) {
                Host receiver = getNonMatchingRandomNodeFromActiveView(sender);
                ShuffleRequestProtocolMessage srm = new ShuffleRequestProtocolMessage(owner, peerSample, timeToLive);
                srm.setFrom(myself);
                sendMessagesLogger.log(Level.INFO, "shufflerequestmessage");
                sendMessage(srm, receiver);

            } else {// a passive view smaller than peer sample
                // Sample to send
                List<Host> replySample = populateSample(new ArrayList<Host>());

                ShuffleReplyProtocolMessage srm = new ShuffleReplyProtocolMessage(replySample);
                srm.setFrom(myself);

                //Send message using TCP temporary connection
                sendMessagesLogger.log(Level.INFO, "shufflereplymessage");
                sendMessageSideChannel(srm, owner);

                //filter peer sample: remove myself, nodes from active and passive view
                peerSample.remove(myself);
                peerSample = filterSample(peerSample, activeView);
                peerSample = filterSample(peerSample, passiveView);

                freeSpaceFromPassiveView(replySample, peerSample.size());

                mergeSamples(peerSample);

            }

            //System.out.println("[SHUFF - REQ] Active: " + activeView.toString());
            //System.out.println("[SHUFF - REQ] Passive: " + passiveView.toString() + "\n");
        }
    };

    private ProtocolMessageHandler uponShuffleReplyMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage protocolMessage) {

            ShuffleReplyProtocolMessage msg = (ShuffleReplyProtocolMessage) protocolMessage;
            List<Host> peerSample = msg.getSample();

            //filter peer sample: remove myself, nodes from active and passive view
            peerSample.remove(myself);
            peerSample = filterSample(peerSample, activeView);
            peerSample = filterSample(peerSample, passiveView);

            freeSpaceFromPassiveView(lastShuffle, peerSample.size());

            mergeSamples(peerSample);

            // System.out.println("[SHUFF - REP] Active: " + activeView.toString());
            // System.out.println("[SHUFF - REP] Passive: " + passiveView.toString() + "\n");
        }
    };

    private void freeSpaceFromPassiveView(List<Host> replySample, int peerSampleSize) {

        if (passiveView.size() + peerSampleSize > PASSIVE_VIEW_MAX_SIZE) {
            int freeSpace = PASSIVE_VIEW_MAX_SIZE - passiveView.size();
            int toRemove = freeSpace - peerSampleSize;

            Random rnd = new Random();

            for (int i = 0; i < toRemove; i++) {
                int index = rnd.nextInt(replySample.size());
                passiveView.remove(replySample.get(index));
            }
        }
    }

    private List<Host> filterSample(List<Host> peerSample, List<Host> myView) {
        for (Host node : myView) {
            peerSample.remove(node);
        }
        return peerSample;
    }

    private Host getNonMatchingRandomNodeFromActiveView(Host node) {
        Random rnd = new Random();
        Host newNode;
        do {
            int index = rnd.nextInt(activeView.size());
            newNode = activeView.get(index);

        } while (newNode.equals(node));

        return newNode;
    }

    private List<Host> chooseRandomFromView(List<Host> newSample, List<Host> view, int limit) {
        Random rnd = new Random();
        for (int i = 0; i < limit; i++) {
            Host node;
            do {
                int index = rnd.nextInt(view.size());
                node = view.get(index);

            } while (newSample.contains(node));

            newSample.add(node);
        }
        return newSample;
    }

    private List<Host> populateSample(List<Host> replySample) {
        int activeNodesToFind = Math.min(activeView.size(), SHUFFLE_KA);
        int passiveNodeToFind = Math.min(passiveView.size(), SHUFFLE_KP);

        if (replySample.isEmpty()) {
            int shuffleSize = 1 + activeNodesToFind + passiveNodeToFind;
            replySample = new ArrayList<>(shuffleSize);
        }

        replySample = chooseRandomFromView(replySample, passiveView, passiveNodeToFind);
        replySample = chooseRandomFromView(replySample, activeView, activeNodesToFind);

        replySample.add(myself);

        return replySample;
    }

    private void mergeSamples(List<Host> peerSample) {
        for (Host peer : peerSample) {
            addNodePassiveView((peer));
        }
    }

    private Set<Host> hostToConnected = new HashSet<Host>();
    private Map<Integer, UUID> hostToConnectTimer = new HashMap<Integer, UUID>();


    @Override
    public void nodeDown(Host host) {
        if (activeView.contains(host)) {
            activeView.remove(host);
            removeNetworkPeer(host);

            if (!passiveView.isEmpty()) {
                tryConnectionWithNodeFromPassiveView();
            }
        }
    }

    private void tryConnectionWithNodeFromPassiveView() {
        Random rnd = new Random();
        Host tryConnectionNode = passiveView.get(rnd.nextInt(passiveView.size()));
        NeighbourRequestProtocolMessage nm = new NeighbourRequestProtocolMessage(myself);
        addNetworkPeer(tryConnectionNode);
        sendMessagesLogger.log(Level.INFO, "neighbourrequestmessage");
        sendMessage(nm, tryConnectionNode);
        hostToConnected.add(tryConnectionNode);
        UUID timerId = setupTimer(new TryConnectionProtocolTimer(tryConnectionNode), TRY_CONNECTION_MS);
        hostToConnectTimer.put(tryConnectionNode.hashCode(), timerId);
    }

    private ProtocolTimerHandler uponConnectionTryTimer = new ProtocolTimerHandler() {
        @Override
        public void uponTimer(ProtocolTimer protocolTimer) {
            TryConnectionProtocolTimer tct = (TryConnectionProtocolTimer) protocolTimer;
            passiveView.remove(tct.getTryConnectionNode());
            hostToConnected.remove(tct.getTryConnectionNode());
            removeNetworkPeer(tct.getTryConnectionNode());
            if (!passiveView.isEmpty()) {
                tryConnectionWithNodeFromPassiveView();
            }
        }
    };

    @Override
    public void nodeUp(Host host) {
        /*if (hostToConnected.contains(host)) {
            hostToConnected.remove(host);
        }*/
    }

    @Override
    public void nodeConnectionReestablished(Host host) {

    }
}