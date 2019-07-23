package com.mongodb.dao.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MonitoredDataAccess implements MongoDataAccess {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private static final int SLEEP_CHECKING_TIME = 3000;

    private final static int RETRY_TIMES = 3;

    private final AtomicBoolean connectionRetry = new AtomicBoolean(true);

    public static final String MONITORING_THREAD = "MongoDBMonitoringThread";

    protected MongoDataAccess dataAccess;
    private String targetDB;

    private MongoClient testClient;

    public MonitoredDataAccess(MongoDataAccess mongoDataAccess, String targetDB) {
        this.dataAccess = mongoDataAccess;
        this.targetDB = targetDB;
    }

    @Override
    public MongoClient client() {
        return dataAccess.client();
    }

    @Override
    public MongoDatabase database(String databaseName) {
        checkConnection();
        return dataAccess.database(databaseName);
    }

    @Override
    public MongoCollection<Document> collection(String databaseName, String collectionName) {
        checkConnection();
        return dataAccess.collection(databaseName, collectionName);
    }

    @Override
    public String shellConnection() {
        checkConnection();
        return dataAccess.shellConnection();
    }

    @Override
    public String shellConnection(String databaseName) {
        checkConnection();
        return dataAccess.shellConnection(databaseName);
    }

    public void checkConnection() {
        boolean isConnected = false;
        int times = 0;
        while (connectionRetry.get() && !isConnected && times < RETRY_TIMES) {
            times++;
            isConnected = isConnected();
        }
        if (!isConnected) {
            if (connectionRetry.get()) {
                testClient = null;
                continuouslyMonitoring();
            }
            throw new RuntimeException("Unavailable connection with MongoDB");
        }
    }

    public boolean isConnected() {
        boolean isConnected = false;
        try {
            isConnected = testClient().getAddress() != null;
        } catch (MongoTimeoutException e) {
            logger.warning("Unavailable MongoDB connection: " + e.getMessage());
        }
        return isConnected;
    }

    private MongoClient testClient() {
        if (testClient == null) {
            testClient = dataAccess.client();
            testClient.getDatabase(targetDB).getCollection("dummy").countDocuments(); // Mongo 3.8.2
//            testClient.getDatabase(targetDB).getCollection("dummy").count(); // Mongo 3.6.0
        }
        return testClient;
    }

    public void continuouslyMonitoring() {
        try {
            logger.log(Level.INFO, "Monitoring Thread...");
            Runnable monitoringSubThread = new MonitoringSubThread(SLEEP_CHECKING_TIME);
            new Thread(monitoringSubThread, MONITORING_THREAD).start();
        } catch (Exception e) {
            logger.warning("Error throwing Monitoring Thread, cause: " + e.getMessage());
        }
    }

    public class MonitoringSubThread implements Runnable {

        private int interval;

        public MonitoringSubThread(int sleepInterval) {
            interval = sleepInterval;
        }

        public void run() {
            connectionRetry.set(false);
            logger.info("Starting to continuously monitoring connection to MongoDB...");
            while (!connectionRetry.get()) {
                if (isConnected()) {
                    connectionRetry.set(true);
                    Thread.currentThread().interrupt();
                } else {
                    logger.info("Retrying to check connection with MongoDB in " + interval + "[ms]");
                }
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("Thread was interrupted.");
                }
            }
            Thread.currentThread().interrupt();
            logger.info("Finished the continuous monitoring connection to MongoDB.");
        }
    }

}