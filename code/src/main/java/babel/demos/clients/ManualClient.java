package babel.demos.clients;

import babel.demos.pubSubService.PubSubService;
import java.util.Scanner;

public class ManualClient {

    static {
        System.setProperty("log4j.configurationFile", "log4j.xml");
    }

    private static final String SUBSCRIBE = "sub";
    private static final String UNSUBSCRIBE = "unsub";
    private static final String PUBLISH = "pub";
    private static final String QUIT = "q";

    public static void main(String[] args) throws Exception {
        PubSubService pubSubService = new PubSubService(args);
        Scanner in = new Scanner(System.in);
        boolean run =  true;
        while (run) {
            System.out.print(">");
            String op = in.next();

            switch (op) {
                case SUBSCRIBE:
                    pubSubService.subscribeTopic(in.next());

                    break;
                case UNSUBSCRIBE:
                    pubSubService.unsubscribeTopic(in.next());

                    break;
                case PUBLISH:
                    String topic = in.next();
                    String msg = in.nextLine();
                    pubSubService.publish(topic, msg.getBytes());
                    break;
                case QUIT:
                    System.exit(0);
                default:
                    System.out.println("Command does not exist.");
                    in.nextLine();
                    break;

            }
        }
        in.close();
    }
}
