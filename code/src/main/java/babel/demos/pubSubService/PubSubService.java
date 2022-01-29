package babel.demos.pubSubService;

import babel.Babel;
import babel.demos.protocols.broadCastWithRecovery.BroadCastWithRecovery;
import babel.demos.protocols.broadCastWithRecovery.notifications.BCastDeliver;
import babel.demos.protocols.chord.Chord;
import babel.demos.protocols.chord.notifications.FindRootNotification;
import babel.demos.protocols.chord.notifications.RouteDelNotification;
import babel.demos.protocols.hyParView.HyParView;
import babel.demos.protocols.pubSubProtocol.PubSubProtocol;
import babel.demos.protocols.pubSubProtocol.notifications.SubscribeMessageNotification;
import babel.demos.protocols.pubSubProtocol.requests.PublishRequest;
import babel.demos.protocols.pubSubProtocol.requests.SubscribeRequest;
import babel.demos.protocols.pubSubProtocol.requests.UnsubscribeRequest;
import babel.demos.protocols.scribe.ScribeProtocol;
import babel.demos.protocols.scribe.notifications.MessageDelivery;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import network.INetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class PubSubService implements INotificationConsumer {

    private static final Logger logger = LogManager.getLogger(PubSubService.class);
    private static final String SUBSCRIBE = "sub";
    private static final String UNSUBSCRIBE = "unsub";
    private static final String PUBLISH = "pub";

    private PubSubProtocol pubSub;

    public PubSubService(String[] args) throws Exception {
        Babel babel = Babel.getInstance();
        Properties configProps = babel.loadConfig("network_config.properties", args);
        INetwork net = babel.getNetworkInstance();

        //Define protocols
        /*HyParView membership = new HyParView(net);
        membership.init(configProps);

        BroadCastWithRecovery bCast = new BroadCastWithRecovery(net);
        bCast.init(configProps);*/

        System.out.println("CHORD creation");

        Chord chord = new Chord(net);
        chord.init(configProps);

        System.out.println("SCRIBE creation");

        ScribeProtocol scribe = new ScribeProtocol(net);
        scribe.init(configProps);
        
        System.out.println("PUBSUB creation");
        
        pubSub = new PubSubProtocol(net);
        pubSub.init(configProps);

        //Register protocols
        babel.registerProtocol(chord);
        babel.registerProtocol(scribe);
        /*babel.registerProtocol(membership);
        babel.registerProtocol(bCast);*/
        babel.registerProtocol(pubSub);

        //subscribe to notifications
        chord.subscribeNotification(RouteDelNotification.NOTIFICATION_ID, scribe);
        chord.subscribeNotification(FindRootNotification.NOTIFICATION_ID, pubSub);

        scribe.subscribeNotification(MessageDelivery.NOTIFICATION_ID, pubSub);
        //bCast.subscribeNotification(BCastDeliver.NOTIFICATION_ID, pubSub);
        pubSub.subscribeNotification(SubscribeMessageNotification.NOTIFICATION_ID, this);


        //start babel runtime
        babel.start();
    }

    public void subscribeTopic(String topic){
        SubscribeRequest sreq = new SubscribeRequest(topic);
        pubSub.deliverRequest(sreq);
    }

    public void unsubscribeTopic(String topic){
        UnsubscribeRequest unsreq = new UnsubscribeRequest(topic);
        pubSub.deliverRequest(unsreq);
    }

    public void publish(String topic, byte[] msg){
        PublishRequest pr = new PublishRequest(topic, msg);
        pubSub.deliverRequest(pr);
    }

    @Override
    public void deliverNotification(ProtocolNotification notification) {
        SubscribeMessageNotification deliver = (SubscribeMessageNotification) notification;
        System.out.println("Received: " + new String(deliver.getMessage(), StandardCharsets.UTF_8));
    }
}
