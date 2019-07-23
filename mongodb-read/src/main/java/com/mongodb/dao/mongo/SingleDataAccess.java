package com.mongodb.dao.mongo;

import com.mongodb.dao.base.DBConnectionString;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.Map;

public class SingleDataAccess implements MongoDataAccess {

    private MongoDBParameters configs;
    private DBConnection connection;
    private Map<String, String> shellConnections;

    public SingleDataAccess(MongoDBParameters configs, DBConnection connection, Map shellConnections) {
        this.configs = configs;
        this.connection = connection;
        this.shellConnections = shellConnections;
    }

    public SingleDataAccess(MongoDBParameters configs) {
        this(configs, new SecureCheckedConnection(new NetworkConnectionString(
                new ConnectionTarget(configs.database,
                        new ConnectionAddress(configs.primary),
                        new ConnectionReplicas(configs.secondary)),
                new ConnectionAuth(configs.username, configs.password, configs.authDatabase,
                        configs.quiet, configs.ssl),
                new ConnectionOptions(configs.authDatabase, configs.replicaSet, configs.readPreferenceControl,
                        configs.connectTimeout, configs.socketTimeout, configs.ssl),
                new ConnectionPoolOptions(configs.maxPoolSize, configs.minPoolSize, configs.maxIdleTime)
        )), new LinkedHashMap<>());
    }

    @Override
    public String shellConnection() {
        return shellConnection(configs.database);
    }

    @Override
    public String shellConnection(String databaseName) {
        if (!shellConnections.containsKey(databaseName)) {
            DBConnectionString connectionString = new CommandlineConnectionString(
                    new ConnectionTarget(databaseName, configs.primary, configs.secondary),
                    new ConnectionAuth(configs.username, configs.password, configs.authDatabase,
                            configs.quiet, configs.ssl),
                    new ConnectionOptions(configs.authDatabase, configs.replicaSet, configs.readPreferenceControl,
                            configs.connectTimeout, configs.socketTimeout, configs.ssl),
                    new ConnectionPoolOptions(configs.maxPoolSize, configs.minPoolSize, configs.maxIdleTime)
            );
            shellConnections.put(databaseName, new DBConnectionString.Auth(connectionString).connectionString());
        }
        return shellConnections.get(databaseName);
    }

    @Override
    public MongoClient client() {
        return connection.mongoClient();
    }

    @Override
    public MongoDatabase database(String databaseName) {
        return connection.mongoDatabase(databaseName);
    }

    @Override
    public MongoCollection<Document> collection(String databaseName, String collectionName) {
        return connection.mongoCollection(databaseName, collectionName);
    }

}
