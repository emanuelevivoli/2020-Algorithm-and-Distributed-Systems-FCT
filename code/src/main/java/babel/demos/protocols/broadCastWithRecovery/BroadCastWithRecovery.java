package babel.demos.protocols.broadCastWithRecovery;

import babel.demos.protocols.broadCastWithRecovery.messages.BCastProtocolMessage;
import babel.demos.protocols.broadCastWithRecovery.messages.CachedIdsReplyProtocolMessage;
import babel.demos.protocols.broadCastWithRecovery.notifications.BCastDeliver;
import babel.demos.protocols.broadCastWithRecovery.requests.BCastRequest;
import babel.demos.protocols.broadCastWithRecovery.timers.CacheRefreshProtocolTimer;
import babel.demos.protocols.broadCastWithRecovery.timers.CacheShareProtocolTimer;
import babel.demos.protocols.hyParView.HyParView;
import babel.demos.protocols.broadCastWithRecovery.messages.CachedIdsRequestProtocolMessage;
import babel.demos.protocols.hyParView.requests.GetPeerNeighboursReply;
import babel.demos.protocols.hyParView.requests.GetPeerNeighboursRequest;
import babel.demos.protocols.pubSubProtocol.PubSubProtocol;
import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.exceptions.HandlerRegistrationException;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolReplyHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolReply;
import babel.requestreply.ProtocolRequest;
import babel.timer.ProtocolTimer;
import network.Host;
import network.INetwork;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.*;

public class BroadCastWithRecovery extends GenericProtocol{

    //Numeric identifier of the protocol
    public final static short PROTOCOL_ID = 2;
    public final static String PROTOCOL_NAME = "BCastWithRecovery";

    static Logger receivedMessagesLogger = Logger.getLogger("ReceivedMessages");
    static Logger sendMessagesLogger = Logger.getLogger("SendMessages");

    private static final String LOG_RECEIVED_FILE = "logs/broadcast/received%s_%s.log";
    private static final String LOG_SEND_FILE = "logs/broadcast/send%s_%s.log";

    public final static short REQUEST_PEERS_TO_BROADCAST = 1;
    public final static short REQUEST_PEERS_TO_CACHE = 2;

    public int message_ttl, fanout;
    public long cache_share_rate, cache_refresh_rate;

    private Host myself;

    private Set<UUID> delivered;
    private Map<UUID, BCastProtocolMessage> pending;
    private Map<UUID, BCastProtocolMessage> cached;


    public BroadCastWithRecovery(INetwork net) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, BroadCastWithRecovery.PROTOCOL_ID, net);
        myself = net.myHost();

        //Requests
        registerRequestHandler(BCastRequest.REQUEST_ID, uponBCastRequest);

        //Replies
        registerReplyHandler(GetPeerNeighboursReply.REPLY_ID, uponGetPeerNeighboursReply);

        //Declare Messages sent/received of the protocol
        registerMessageHandler(BCastProtocolMessage.MSG_CODE, uponBCastMessage, BCastProtocolMessage.serializer);
        registerMessageHandler(CachedIdsRequestProtocolMessage.MSG_CODE, uponCachedIdsRequestProtocolMessage, CachedIdsRequestProtocolMessage.serializer);
        registerMessageHandler(CachedIdsReplyProtocolMessage.MSG_CODE, uponCachedIdsReplyProtocolMessage, CachedIdsReplyProtocolMessage.serializer);

        //Notification
        registerNotification(BCastDeliver.NOTIFICATION_ID, BCastDeliver.NOTIFICATION_NAME);

        //Timers
        registerTimerHandler(CacheRefreshProtocolTimer.TimerCode, refreshCache);
        registerTimerHandler(CacheShareProtocolTimer.TimerCode, shareCache);


        try {

            FileHandler receivedfh = new FileHandler(String.format(LOG_RECEIVED_FILE, myself.getAddress(), myself.getPort()));
            receivedMessagesLogger.addHandler(receivedfh);
            SimpleFormatter formatter = new SimpleFormatter();
            receivedfh.setFormatter(formatter);
            FileHandler sendfh = new FileHandler(String.format(LOG_SEND_FILE, myself.getAddress(), myself.getPort()));
            sendMessagesLogger.addHandler(sendfh);

            sendfh.setFormatter(formatter);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int getFanout() {
        return fanout;
    }

    @Override
    public void init(Properties props) {

        //Setup configuration of the protocol
        cache_share_rate = Long.parseLong(props.getProperty("cache_share_rate", "5000"));
        cache_refresh_rate = Long.parseLong(props.getProperty("cache_refresh_rate", "5000"));
        message_ttl = Integer.parseInt(props.getProperty("message_ttl", "7"));
        short sampleSize = Short.parseShort(props.getProperty("SampleSize", "8"));
        short defaultFanout = (short) Math.log(((double) sampleSize));
        fanout = Short.parseShort(props.getProperty("Funout", "" + defaultFanout));

        delivered = new TreeSet<>();
        pending = new HashMap<>();
        cached = new ConcurrentHashMap<>();

        setupPeriodicTimer(new CacheRefreshProtocolTimer(), cache_refresh_rate, cache_refresh_rate);
        setupPeriodicTimer(new CacheShareProtocolTimer(), cache_share_rate, cache_share_rate);
    }

    private ProtocolRequestHandler uponBCastRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest r) {
            //Create Message
            BCastRequest req = (BCastRequest) r;
            BCastProtocolMessage message = new BCastProtocolMessage(message_ttl);
            message.setPayload(req.getPayload());
            message.setFrom(myself);

            //Get peer list
            GetPeerNeighboursRequest request = new GetPeerNeighboursRequest(message.getMessageId(), getFanout(), null, REQUEST_PEERS_TO_BROADCAST);
            request.setDestination(HyParView.PROTOCOL_ID);
            pending.put(message.getMessageId(), message);
            try {
                sendRequest(request);
            } catch (DestinationProtocolDoesNotExist e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    };

    private ProtocolReplyHandler uponGetPeerNeighboursReply = new ProtocolReplyHandler() {
        @Override
        public void uponReply(ProtocolReply reply) {

            GetPeerNeighboursReply rep = (GetPeerNeighboursReply) reply;
            List<Host> peerList = rep.getSample();

            if (rep.getReplyType() == REQUEST_PEERS_TO_BROADCAST) {
                BCastProtocolMessage msg = pending.remove(rep.getRequestID());

                if (msg == null) {
                    return;
                }

                //Send messages all neighbours.
                for (Host peer : peerList) {
                    sendMessagesLogger.log(Level.INFO,  msg.getMessageId().toString());
                    sendMessage(msg, peer);
                }

                //Deliver message notification
                delivered.add(msg.getMessageId());
                cached.put(msg.getMessageId(), msg);

                receivedMessagesLogger.log(Level.INFO,  msg.getMessageId().toString());

                BCastDeliver deliver = new BCastDeliver(msg.getPayload());
                triggerNotification(deliver);


            } else if (rep.getReplyType() == REQUEST_PEERS_TO_CACHE) {
                for (Host peer : peerList) {
                    CachedIdsRequestProtocolMessage request = new CachedIdsRequestProtocolMessage(cached.keySet());
                    sendMessage(request, peer);
                }
            }

        }
    };

    private ProtocolMessageHandler uponBCastMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage protocolMessage) {
            BCastProtocolMessage msg = (BCastProtocolMessage) protocolMessage;
            //check if messages was already observed, ignore if yes
            if (!delivered.contains(msg.getMessageId())) {
                receivedMessagesLogger.log(Level.INFO,  msg.getMessageId().toString());
                //Create Request for peers (in the membership)
                GetPeerNeighboursRequest request = new GetPeerNeighboursRequest(msg.getMessageId(), getFanout(), msg.getFrom(), REQUEST_PEERS_TO_BROADCAST);
                request.setDestination(HyParView.PROTOCOL_ID);
                pending.put(msg.getMessageId(), msg);
                try {
                    sendRequest(request);
                } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
                    destinationProtocolDoesNotExist.printStackTrace();
                    System.exit(1);
                }
            }
        }
    };

    private ProtocolMessageHandler uponCachedIdsRequestProtocolMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage protocolMessage) {

            CachedIdsRequestProtocolMessage msg = (CachedIdsRequestProtocolMessage) protocolMessage;

            Set<UUID> missingMgs = new HashSet<UUID>();

            Iterator<UUID> it = msg.getCachedIds().iterator();
            while (it.hasNext()) {
                UUID id = it.next();
                if (!cached.containsKey(id)) {
                    missingMgs.add(id);
                }
            }

            if (missingMgs.size() > 0) {
                CachedIdsReplyProtocolMessage replyMsg = new CachedIdsReplyProtocolMessage(missingMgs);
                sendMessageSideChannel(replyMsg, msg.getFrom());
            }
        }
    };


    private ProtocolMessageHandler uponCachedIdsReplyProtocolMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage protocolMessage) {
            CachedIdsReplyProtocolMessage msg = (CachedIdsReplyProtocolMessage) protocolMessage;
            Iterator<UUID> it = msg.getNotReceivedIds().iterator();
            while (it.hasNext()) {
                BCastProtocolMessage bMsg = cached.get(it.next());
                if (bMsg != null) {
                    sendMessagesLogger.log(Level.INFO,  bMsg.getMessageId().toString());
                    sendMessageSideChannel(bMsg, msg.getFrom());
                }
            }
        }
    };

    private ProtocolTimerHandler refreshCache = new ProtocolTimerHandler() {
        @Override
        public void uponTimer(ProtocolTimer protocolTimer) {
            Iterator<UUID> it = cached.keySet().iterator();
            while (it.hasNext()) {
                UUID key = (UUID) it.next();
                BCastProtocolMessage msg = cached.get(key);
                msg.decrementTll();
                if (msg.getTimeStamp() <= 0)
                    cached.remove(key);
            }
        }

    };

    private ProtocolTimerHandler shareCache = new ProtocolTimerHandler() {
        @Override
        public void uponTimer(ProtocolTimer protocolTimer) {
            GetPeerNeighboursRequest request = new GetPeerNeighboursRequest(UUID.randomUUID(), getFanout(), myself, REQUEST_PEERS_TO_CACHE);
            request.setDestination(HyParView.PROTOCOL_ID);
            try {
                sendRequest(request);
            } catch (DestinationProtocolDoesNotExist e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    };
}