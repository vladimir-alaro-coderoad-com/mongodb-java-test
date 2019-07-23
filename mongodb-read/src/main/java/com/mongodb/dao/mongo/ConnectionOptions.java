package com.mongodb.dao.mongo;

import java.util.ArrayList;
import java.util.logging.Logger;

public class ConnectionOptions {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private String authDatabase;
    private String replicaSet;
    private String readPreference;
    private boolean ssl;
    private int connectTimeoutMS;
    private int socketTimeoutMS;

    public ConnectionOptions(String authDatabase, String replicaSet, String readPreference, int connectTimeoutMS,
                             int socketTimeoutMS, boolean ssl) {
        this.authDatabase = authDatabase;
        this.replicaSet = replicaSet;
        this.readPreference = readPreference;
        this.ssl = ssl;
        this.connectTimeoutMS = connectTimeoutMS;
        this.socketTimeoutMS = socketTimeoutMS;
    }

    @SuppressWarnings("unchecked")
    public String commandOptions() {
        ArrayList options = new ArrayList();
        if (!replicaSet.isEmpty()) {
            options.add("replicaSet=" + replicaSet);
        }
        if (!readPreference.isEmpty()) {
            options.add("readPreference=" + readPreference);
        }
        options.add("ssl=" + (ssl ? "true" : "false"));
        options.add("connectTimeoutMS=" + connectTimeoutMS);
        options.add("socketTimeoutMS=" + socketTimeoutMS);
        return String.join("&", options);
    }

    @SuppressWarnings("unchecked")
    public String urlOptions() {
        ArrayList options = new ArrayList();
        if (!replicaSet.isEmpty()) {
            options.add("replicaSet=" + replicaSet);
        }
        if (!authDatabase.isEmpty()) {
            options.add("authSource=" + authDatabase);
        }
        if (!readPreference.isEmpty()) {
            options.add("readPreference=" + readPreference);
        }
        options.add("ssl=" + (ssl ? "true" : "false"));
        options.add("connectTimeoutMS=" + connectTimeoutMS);
        options.add("socketTimeoutMS=" + socketTimeoutMS);
        return String.join("&", options);
    }

    public int connectionTimeout() {
        return connectTimeoutMS;
    }

}
