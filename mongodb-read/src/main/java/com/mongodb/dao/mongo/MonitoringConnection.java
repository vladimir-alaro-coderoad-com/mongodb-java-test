package com.mongodb.dao.mongo;

import com.mongodb.dao.base.DBConnectionString;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.logging.Logger;

public class MonitoringConnection implements DBConnection {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    public final static int DEFAULT_SERVER_SELECTION_TIMEOUT = 5000;

    private DBConnection connection;
    private int serverSelectionTimeout;

    public MonitoringConnection(DBConnection connection, int serverSelectionTimeout) {
        this.connection = connection;
        this.serverSelectionTimeout = serverSelectionTimeout;
    }

    public MonitoringConnection(DBConnectionString connectionString, int serverSelectionTimeout) {
        this(new Connection(connectionString,
                        new MongoClient(new MongoClientURI(connectionString.connectionString(true),
                                MongoClientOptions.builder().serverSelectionTimeout(serverSelectionTimeout)))),
                serverSelectionTimeout);
    }

    public MonitoringConnection(DBConnectionString connectionString) {
        this(connectionString, DEFAULT_SERVER_SELECTION_TIMEOUT);
    }

    @Override
    public MongoClient mongoClient() {
        return connection.mongoClient();
    }

    @Override
    public MongoDatabase mongoDatabase(String databaseName) {
        return connection.mongoDatabase(databaseName);
    }

    @Override
    public MongoCollection mongoCollection(String databaseName, String collectionName) {
        return connection.mongoCollection(databaseName, collectionName);
    }

}