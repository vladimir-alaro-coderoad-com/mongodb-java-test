package com.mongodb.dao.mongo;

import com.mongodb.dao.base.DBConnectionString;
import com.mongodb.dao.base.DBCredentials;
import com.mongodb.dao.base.DBCredentialsPool;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MultiDataAccess implements MongoDataAccess {

    private MongoDBParameters configs;
    private DBCredentialsPool credentialsPool;
    private Map<String, DBConnection> connectionsMap;
    private Map<String, String> shellConnectionsMap;
    private String rootKey;

    /**
     * Primary ctr
     *
     * @param mongoDBGlobalConfigs
     * @param credentialsPool
     * @param connectionsMap
     * @param shellConnectionsMap
     */
    public MultiDataAccess(MongoDBParameters mongoDBGlobalConfigs, DBCredentialsPool credentialsPool,
                           Map connectionsMap, Map shellConnectionsMap) {
        this.configs = mongoDBGlobalConfigs;
        this.credentialsPool = credentialsPool;
        this.connectionsMap = connectionsMap;
        this.shellConnectionsMap = shellConnectionsMap;
        this.rootKey = configs.database;
    }

    public MultiDataAccess(MongoDBParameters mongoDBGlobalConfigs, DBCredentialsPool credentialsPool) {
        this(mongoDBGlobalConfigs, credentialsPool, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        this.connectionsMap.put(rootKey, new SecureCheckedConnection(new NetworkConnectionString(
                new ConnectionTarget(configs.database,
                        new ConnectionAddress(configs.primary),
                        new ConnectionReplicas(configs.secondary)),
                new ConnectionAuth(configs.username, configs.password, configs.authDatabase,
                        configs.quiet, configs.ssl),
                new ConnectionOptions(configs.authDatabase, configs.replicaSet, configs.readPreferenceControl,
                        configs.connectTimeout, configs.socketTimeout, configs.ssl),
                new ConnectionPoolOptions(configs.maxPoolSize, configs.minPoolSize, configs.maxIdleTime)
        )));
        this.shellConnectionsMap.put(rootKey, new DBConnectionString.Auth(new CommandlineConnectionString(
                new ConnectionTarget(configs.database, configs.primary),
                new ConnectionAuth(configs.username, configs.password, configs.authDatabase,
                        configs.quiet, configs.ssl),
                new ConnectionOptions(configs.authDatabase, configs.replicaSet, configs.readPreferenceReports,
                        configs.connectTimeout, configs.socketTimeout, configs.ssl),
                new ConnectionPoolOptions(configs.maxPoolSize, configs.minPoolSize, configs.maxIdleTime)
        )).connectionString());
    }


    @Override
    public String shellConnection() {
        return shellConnection(rootKey);
    }

    @Override
    public String shellConnection(String databaseName) {
        if (!shellConnectionsMap.containsKey(databaseName)) {
            DBCredentials accessTenantDB = credentialsPool.credentials(databaseName);
            if (accessTenantDB == null) {
                throw new RuntimeException("Not found credentials for database '" + databaseName + "'");
            }
            DBConnectionString connectionString = new CommandlineConnectionString(
                    new ConnectionTarget(databaseName, configs.primary, configs.secondary),
                    new ConnectionAuth(accessTenantDB.getUsername(), accessTenantDB.getPassword(), databaseName,
                            configs.quiet, configs.ssl),
                    new ConnectionOptions(databaseName, configs.replicaSet, configs.readPreferenceReports,
                            configs.connectTimeout, configs.socketTimeout, configs.ssl),
                    new ConnectionPoolOptions(configs.maxPoolSize, configs.minPoolSize, configs.maxIdleTime)
            );
            shellConnectionsMap.put(databaseName, new DBConnectionString.Auth(connectionString).connectionString());
        }
        return shellConnectionsMap.get(databaseName);
    }

    @Override
    public MongoClient client() {
        return dbConnection(rootKey).mongoClient();
    }

    @Override
    public MongoDatabase database(String databaseName) {
        return dbConnection(databaseName).mongoDatabase(databaseName);
    }

    @Override
    public MongoCollection<Document> collection(String databaseName, String collectionName) {
        return dbConnection(databaseName).mongoCollection(databaseName, collectionName);
    }

    private DBConnection dbConnection(String database) {
        if (!connectionsMap.containsKey(database)) {
            DBCredentials accessTenantDB = credentialsPool.credentials(database);
            if (accessTenantDB == null) {
                throw new RuntimeException("Not found credentials for database '" + database + "'");
            }
            connectionsMap.put(database, new MappedConnection(new NetworkConnectionString(
                    new ConnectionTarget(accessTenantDB.getDatabase(), configs.primary, configs.secondary),
                    new ConnectionAuth(accessTenantDB.getUsername(), accessTenantDB.getPassword(),
                            database, configs.quiet, configs.ssl),
                    new ConnectionOptions(database, configs.replicaSet, configs.readPreferenceControl,
                            configs.connectTimeout, configs.socketTimeout, configs.ssl),
                    new ConnectionPoolOptions(configs.maxPoolSize, configs.minPoolSize, configs.maxIdleTime)
            )));
        }
        return connectionsMap.get(database);
    }

}
