package com.mongodb.dao.mongo;

import java.util.logging.Logger;

public class ConnectionTarget {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private String database;
    private ConnectionAddress connectionAddress;
    private ConnectionReplicas connectionReplicas;
    private String target = null;

    public ConnectionTarget(String database, ConnectionAddress connectionAddress, ConnectionReplicas connectionReplicas) {
        this.database = database;
        this.connectionAddress = connectionAddress;
        this.connectionReplicas = connectionReplicas;
    }

    public ConnectionTarget(String database, ConnectionAddress connectionAddress) {
        this(database, connectionAddress, null);
    }

    public ConnectionTarget(String database, String host, int port, String replicaAddresses) {
        this(database, new ConnectionAddress(host, port), new ConnectionReplicas(replicaAddresses));
    }

    public ConnectionTarget(String database, String server, String replicaAddresses) {
        this(database, new ConnectionAddress(server), new ConnectionReplicas(replicaAddresses));
    }

    public ConnectionTarget(String database, String host, int port) {
        this(database, new ConnectionAddress(host, port));
    }

    public ConnectionTarget(String database, String server) {
        this(database, new ConnectionAddress(server));
    }

    public String target() {
        if (target == null) {
            target = connectionAddress.urlServerAddress() +
                    (connectionReplicas != null ? connectionReplicas.replicasTarget() : "") +
                    "/" + database;
        }
        return target;
    }

}
