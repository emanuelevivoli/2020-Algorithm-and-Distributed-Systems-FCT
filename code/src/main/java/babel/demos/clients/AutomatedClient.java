package babel.demos.clients;

import babel.demos.pubSubService.PubSubService;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Logger;

public class AutomatedClient {

    static {
        System.setProperty("log4j.configurationFile", "log4j.xml");
    }

    private static final String SUBSCRIBE = "sub";
    private static final String UNSUBSCRIBE = "unsub";
    private static final String PUBLISH = "pub";
    private static final String[] commands = {SUBSCRIBE, UNSUBSCRIBE, PUBLISH};
    private static List<String> topics = new LinkedList<String>(), msgs = new LinkedList<String>();
    private static int timer, inicialSleep, msgsToSend;
    //static Logger logger = Logger.getLogger(AutomatedClient.class.getName());


    public static void main(String[] args) throws Exception {
        PubSubService pubSubService = new PubSubService(args);
        readTopicsAndMsgFromFile(".\\src\\main\\java\\babel\\demos\\clients\\autoclient_configs.txt");
        Thread.sleep(inicialSleep);
        Thread.sleep(50000);
        for (int i = 0; i < msgsToSend; i++) {
            Random rng = new Random();
            String op = commands[/*rng.nextInt(3)*/2];
            String topic = topics.get(rng.nextInt(topics.size()));
            switch (op) {
                case SUBSCRIBE:
                    pubSubService.subscribeTopic(topic);
                    //logger.info("Subscribed: " + topic);
                    break;
                case UNSUBSCRIBE:
                    pubSubService.unsubscribeTopic(topic);
                    //logger.info("Unsubscribed: " + topic);
                    break;
                case PUBLISH:
                    String msg = msgs.get(rng.nextInt(msgs.size()));
                    pubSubService.publish(topic, msg.getBytes());
                    //logger.info("Publish: " + topic + "->" + msg);
                    break;
                default:
                    break;
            }
            Thread.sleep(timer);
        }
    }

    private static final void readTopicsAndMsgFromFile(String configFile) {
        try {
            InputStream fin = new FileInputStream(configFile);
            Scanner in = new Scanner(fin);
            int numTopic = in.nextInt();
            int numMessages = in.nextInt();
            timer = in.nextInt();
            msgsToSend = in.nextInt();
            inicialSleep = in.nextInt();
            in.nextLine();

            for (int i = 0; i < numTopic; i++) {
                String a = in.nextLine();
                topics.add(a);
            }

            for (int i = 0; i < numMessages; i++) {
                msgs.add(in.nextLine());
            }
            in.close();
        } catch (FileNotFoundException e) {
            System.out.println("AutoClient configuration file was not found.");
            System.exit(-1);
        }
    }
}
