package babel.demos.statistics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

public class StatisticCalculator {

    public static void main(String argv[]) throws FileNotFoundException {

        int numberNodeInNetwork = Integer.parseInt(argv[0]);
        boolean received = false;
        Map<String, Set<String>> receivedMsgs = new HashMap<String, Set<String>>();
        Set<String> allDistinctMsgs = new HashSet<>();
        int[] totalReceivedMessages = new int[numberNodeInNetwork];
        int[] totalReceivedDistinctMessages = new int[numberNodeInNetwork];
        int[] totalMissedMessages = new int[numberNodeInNetwork];
        int[] totalSendMessages = new int[numberNodeInNetwork];
        int  totalMsgs = 0, totalLost = 0, totalSentHyparview = 0;
        float ratio = 0;


        for (int i = 0; i < numberNodeInNetwork; i++) {
            received = !received;

            int port = 10001 + i;
            File logsFile;
            String currNode = "127.0.0.1:" + port;

            logsFile = new File("logs/broadcast/received/127.0.0.1_" + port + ".log");
            Scanner in = new Scanner(new FileInputStream(logsFile));

            receivedMsgs.put(currNode, new HashSet<>());
            while (in.hasNext()) {
                in.nextLine();
                in.next();
                String msg = in.nextLine().trim();
                receivedMsgs.get(currNode).add(msg);
                allDistinctMsgs.add(msg);
                totalReceivedMessages[i]++;
                totalMsgs ++;
            }

            logsFile = new File("logs/broadcast/send/127.0.0.1_" + port + ".log");
            in = new Scanner(new FileInputStream(logsFile));
            while (in.hasNext()) {
                in.nextLine();
                in.next();
                String msg = in.nextLine().trim();
                totalSendMessages[i]++;
            }

            //Hyparview
            logsFile = new File("logs/hyparview/send/127.0.0.1_" + port + ".log");
            in = new Scanner(new FileInputStream(logsFile));
            while (in.hasNext()) {
                in.nextLine();
                in.nextLine();
                totalSentHyparview++;
            }

        }

        Iterator<String> keySetIterator = receivedMsgs.keySet().iterator();
        int index = 0;
        while (keySetIterator.hasNext()) {
            String key = keySetIterator.next();
            int numberDistinctReceivedMsgs = receivedMsgs.get(key).size();
            totalReceivedDistinctMessages[index] = numberDistinctReceivedMsgs;
            totalLost += allDistinctMsgs.size() - numberDistinctReceivedMsgs;
            index++;
        }
        ratio = (float)allDistinctMsgs.size()*numberNodeInNetwork / (float)(allDistinctMsgs.size()*numberNodeInNetwork + totalLost);

        System.out.println("Broadcast ----------------------------------");
        System.out.println("Total of Received Messages: " + totalMsgs);
        System.out.println("Total of Received Distinct Messages: " + allDistinctMsgs.size());
        System.out.println("Total of Lost Messages: " + totalLost);
        System.out.println("Ratio: " + ratio * 100);

        System.out.println("Hyparview ----------------------------------");
        System.out.println("Total of Hyparview Messages Sent: " + totalSentHyparview);
    }
}
