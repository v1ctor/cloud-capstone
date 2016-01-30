package org.buldakov;

import org.buldakov.common.CassandraClient;

public class CassandraTester {

    public static void main(String[] args) throws InterruptedException {

        String host = "";

        CassandraClient client = new CassandraClient();

        //Create the connection
        client.createConnection(host);

        //Add test value
        client.addKey("test1234");

        //Close the connection
        client.closeConnection();

        System.out.println("Write Complete");
    }

}
