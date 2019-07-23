package com.mongodb.dao.mongo;

import com.mongodb.dao.base.DBConnectionString;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SecureCheckedConnection implements DBConnection {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private DBConnectionString connectionString;
    private DBConnection connection;
    private boolean checked = false;

    public SecureCheckedConnection(DBConnectionString connectionString, DBConnection connection) {
        this.connectionString = connectionString;
        this.connection = connection;
    }

    public SecureCheckedConnection(DBConnectionString connectionString) {
        this(connectionString, new MappedConnection(connectionString));
    }

    @Override
    public MongoClient mongoClient() {
        checkServer();
        return connection.mongoClient();
    }

    @Override
    public MongoDatabase mongoDatabase(String databaseName) {
        checkServer();
        return connection.mongoDatabase(databaseName);
    }

    @Override
    public MongoCollection mongoCollection(String databaseName, String collectionName) {
        checkServer();
        return connection.mongoCollection(databaseName, collectionName);
    }

    @Override
    public String toString() {
        return connection.toString();
    }

    private void checkServer() {
        while (!checked) {
            try {
                checkServerAuthentication();
                checked = true;
            } catch (SecurityException e) {
                logger.log(Level.WARNING, e.getMessage());
                throw e;
            } catch (Exception e) {
                if (e.getMessage().contains("com.mongodb.MongoSecurityException")) {
                    logger.log(Level.SEVERE, "Mongo authentication failed");
                    throw new MongoSecurityException(
                            new MongoClient(new MongoClientURI(
                                    new DBConnectionString.Auth(connectionString).connectionString())
                            ).getCredentialsList().get(0),
                            "Mongo authentication failed.", e);
                }
                int connectionTimeout = connectionString.options().connectionTimeout();
                logger.log(Level.WARNING, "Startup process for [Services] waiting for [MongoDB], retry in " + (connectionTimeout == 0 ? "30" : connectionTimeout) + "s");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void checkServerAuthentication() throws SecurityException {
        MongoClient mongoClient = null;
        try {
            new MongoClient(new MongoClientURI(connectionString.connectionString(false))).
                    listDatabases().first();
            logger.log(Level.WARNING, "Mongo Authentication in host isn't enabled");
            throw new SecurityException("Mongo Authentication not enabled in host");
        } catch (MongoCommandException e) {
            logger.log(Level.INFO, "Mongo Authentication in host is enabled");
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage() + ", when try to check Server Authentication with: " +
                    connectionString.connectionString(false), e);
            throw e;
        } finally {
            if (mongoClient != null) {
                try {
                    mongoClient.close();
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }
    }

}

