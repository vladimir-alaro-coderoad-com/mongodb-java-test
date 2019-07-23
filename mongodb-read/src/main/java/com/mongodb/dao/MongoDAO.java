package com.mongodb.dao;

import com.mongodb.dao.mongo.*;
import com.mongodb.util.Constants;
import com.mongodb.util.PropertiesService;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.LinkedHashMap;
import java.util.logging.Logger;

@ApplicationScoped
public class MongoDAO implements MongoDataAccess {

    private static final Logger logger = Logger.getLogger(MongoDAO.class.getName());

    @Inject
    PropertiesService propertiesService;

    protected MongoDataAccess dataAccess;

    @PostConstruct
    public void initialize() {
        MongoDBParameters configs = new MongoDBParameters(propertiesService);
        if (propertiesService.get("unit_test") != null && !propertiesService.get("unit_test").isEmpty()) {
            configs.primary = "127.0.0.1:27020"; // Always 27020 for unit testing
            dataAccess = new SingleDataAccess(configs, new MappedConnection(new NetworkConnectionString(
                    new ConnectionTarget(configs.database,
                            new ConnectionAddress(configs.primary)),
                    null,
                    null,
                    null
            )), new LinkedHashMap<>());
        } else {
            NetworkConnectionString networkConnectionString = new NetworkConnectionString(
                    new ConnectionTarget(configs.database,
                            new ConnectionAddress(configs.primary),
                            new ConnectionReplicas(configs.secondary)),
                    new ConnectionAuth(configs.username, configs.password, configs.authDatabase,
                            configs.quiet, configs.ssl),
                    new ConnectionOptions(configs.authDatabase, configs.replicaSet, configs.readPreferenceControl,
                            configs.connectTimeout, configs.socketTimeout, configs.ssl),
                    new ConnectionPoolOptions(configs.maxPoolSize, configs.minPoolSize, configs.maxIdleTime)
            );
            DBConnection connection;
            if (configs.ssl) {
                connection = new MappedConnection(new FakedSslConnection(networkConnectionString));
            } else {
                connection = new MappedConnection(networkConnectionString);
            }
            dataAccess = new MonitoredDataAccess(new SingleDataAccess(configs, connection, new LinkedHashMap<>()), configs.database);
        }
    }

    public void verifyDBConnection() {
        try {
            String databaseName = propertiesService.get(Constants.DEFAULT_DATABASE_KEY);
            logger.info("********** CONNECTING TO MONGODB SEVER");
            dataAccess.database(databaseName);
            logger.info("********** CONNECTED SUCCESSFULLY WITH MONGODB : " + dataAccess.toString());
        } catch (Exception e) {
            logger.info("********** MONGODB CONNECTION ERROR [" + e.getMessage() + "] :" + dataAccess.toString());
        }
    }

    @Override
    public MongoClient client() {
        return dataAccess.client();
    }

    @Override
    public MongoDatabase database(String databaseName) {
        return dataAccess.database(databaseName);
    }

    @Override
    public MongoCollection<Document> collection(String databaseName, String collectionName) {
        return dataAccess.collection(databaseName, collectionName).withWriteConcern(WriteConcern.ACKNOWLEDGED);
    }

    @Override
    public String shellConnection() {
        return dataAccess.shellConnection();
    }

    @Override
    public String shellConnection(String databaseName) {
        return dataAccess.shellConnection(databaseName);
    }

}
