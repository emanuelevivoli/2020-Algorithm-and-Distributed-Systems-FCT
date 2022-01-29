package babel.demos.statistics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class LoadBalanceStats {


    public static void main(String[] args) throws FileNotFoundException {
        int numberOfNodes = 30; //10, 20, 30
        int numberOfTopics = 30;
        int expectedTopicsPerNode = numberOfTopics / numberOfNodes;

        int[] topicsPerNode = new int[numberOfNodes];
        int maxTopicsPerNode = 0;
        int minTopicsPerNode = numberOfTopics;


        for( int i = 0; i < numberOfNodes; i++){
            int port = 10001 + i;
            File logsFile;
            String currNode = "127.0.0.1:" + port;

            logsFile = new File("logs/chord/loadBalancing/127.0.0.1_" + port + ".log");
            Scanner in = new Scanner(new FileInputStream(logsFile));

            while(in.hasNext()){
                topicsPerNode[i] += 1;
                in.nextLine();
            }
            topicsPerNode[i] = topicsPerNode[i] / 2;

            if(topicsPerNode[i] < minTopicsPerNode){
                minTopicsPerNode = topicsPerNode[i];
            }

            if(topicsPerNode[i] > maxTopicsPerNode){
                maxTopicsPerNode = topicsPerNode[i];
            }

        }

        System.out.println("Expected topics per node: " + expectedTopicsPerNode);
        System.out.println("Maximum topics per node: " + maxTopicsPerNode);
        System.out.println("Minimum topics per node: " + minTopicsPerNode);
    }
}
