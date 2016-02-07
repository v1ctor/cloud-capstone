package org.buldakov.common;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class CassandraClient {

    private Cluster cluster;
    private Session session;

    public Session getSession()  {
        if (this.session == null && (this.cluster == null || this.cluster.isClosed())) {
        } else if (this.session.isClosed()) {
            this.session = this.cluster.connect();
        }

        return this.session;
    }

    public void createConnection(String node)  {
        this.cluster = Cluster.builder().addContactPoint(node).build();

        Metadata metadata = cluster.getMetadata();

        System.out.printf("Connected to cluster: %s\n",metadata.getClusterName());

        for ( Host host : metadata.getAllHosts() ) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        this.session = cluster.connect();
    }

    public void closeConnection() {
        cluster.close();
    }

    public void execute(Statement statement) {
        Session session = this.getSession();
        try {
            session.execute(statement);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
