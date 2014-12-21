package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by vaibhav on 12/20/14.
 */

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        List<String> servers = Arrays.asList("http://localhost:3000",
                "http://localhost:3001",
                "http://localhost:3002");

        System.out.println("Initiating Write");
        final CRDTClient writeCrdtClient = new CRDTClient(servers);
        writeCrdtClient.put(1, "a");
        Thread.sleep(30000);
        final CRDTClient readOnWriteCrdtClient = new CRDTClient(servers);
        readOnWriteCrdtClient.put(1,"b");
        Thread.sleep(30000);
        final CRDTClient readOnWriteCrdtClient2 = new CRDTClient(servers);
        System.out.println("Initiating Read repair");
        readOnWriteCrdtClient2.get(1);
        System.out.println("Existing Cache Client...");
    }

}
