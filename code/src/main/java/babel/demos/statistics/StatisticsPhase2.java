package babel.demos.statistics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class StatisticsPhase2 {

    public static void main(String[] args) throws FileNotFoundException {

        int totalScribeReceived = 0;
        int numberOfNodes = 21;

        for( int i = 0 ; i < numberOfNodes; i++){

            int port = 10001 + i;
            File logsFile;
            String currNode = "127.0.0.1:" + port;
            int totalSent = 0;

            logsFile = new File("logs/scribe/received/127.0.0.1_" + port + ".log");
            Scanner in = new Scanner(new FileInputStream(logsFile));

            while(in.hasNext()){
                totalSent++;
                in.nextLine();
            }

            totalScribeReceived += (totalSent / 2);
        }

        System.out.println("Total Scribe Messages Received: " + totalScribeReceived);

    }
}
