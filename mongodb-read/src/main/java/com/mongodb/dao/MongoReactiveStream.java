package com.mongodb.dao;

import com.mongodb.ConnectionString;
import com.mongodb.dao.base.DBConnectionString;
import com.mongodb.dao.mongo.*;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.util.PropertiesService;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.logging.Logger;

public class MongoReactiveStream {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    @Inject
    PropertiesService propertiesService;

    protected MongoClient mongoClient;

    @PostConstruct
    public void initialize() {
        MongoDBParameters configs = new MongoDBParameters(propertiesService);
        DBConnectionString connectionString = new NetworkConnectionString(
                new ConnectionTarget(configs.database,
                        new ConnectionAddress(configs.primary),
                        new ConnectionReplicas(configs.secondary)),
                new ConnectionAuth(configs.username, configs.password, configs.authDatabase,
                        configs.quiet, configs.ssl),
                new ConnectionOptions(configs.authDatabase, configs.replicaSet, configs.readPreferenceControl,
                        configs.connectTimeout, configs.socketTimeout, configs.ssl),
                new ConnectionPoolOptions(configs.maxPoolSize, configs.minPoolSize, configs.maxIdleTime)
        );
        String urlConnection = new DBConnectionString.Auth(connectionString).connectionString();
        mongoClient = MongoClients.create(new ConnectionString(urlConnection));

    }

    public MongoCollection collection(String databaseName, String collectionName) {
        return mongoClient.getDatabase(databaseName).getCollection(collectionName);
    }
}

