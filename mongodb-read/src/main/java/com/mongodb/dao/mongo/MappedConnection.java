package com.mongodb.dao.mongo;

import com.mongodb.dao.base.DBConnectionString;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class MappedConnection implements DBConnection {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private DBConnection connection;
    private Map<String, MongoDatabase> databasesMap;
    private Map<String, MongoCollection> collectionsMap;

    public MappedConnection(DBConnection connection, Map databasesMap, Map collectionsMap) {
        this.connection = connection;
        this.databasesMap = databasesMap;
        this.collectionsMap = collectionsMap;
    }

    public MappedConnection(DBConnection dbConnection) {
        this(dbConnection, new ConcurrentHashMap(), new ConcurrentHashMap());
    }

    public MappedConnection(DBConnectionString dbConnectionString) {
        this(new Connection(dbConnectionString));
    }

    @Override
    public MongoClient mongoClient() {
        return connection.mongoClient();
    }

    @Override
    public MongoDatabase mongoDatabase(String databaseName) {
        if (!databasesMap.containsKey(databaseName)) {
            databasesMap.put(databaseName, connection.mongoDatabase(databaseName));
        }
        return databasesMap.get(databaseName);
    }

    @Override
    public MongoCollection mongoCollection(String databaseName, String collectionName) {
        String collectionKey = databaseName + "___" + collectionName;
        if (!collectionsMap.containsKey(collectionKey)) {
            collectionsMap.put(collectionKey, connection.mongoCollection(databaseName, collectionName));
        }
        return collectionsMap.get(collectionKey);
    }

    @Override
    public String toString() {
        return connection.toString();
    }

}
