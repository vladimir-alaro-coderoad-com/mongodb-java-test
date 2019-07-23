package com.mongodb.dao.mongo;

import com.mongodb.dao.base.DBConnectionString;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FakedSslConnection implements DBConnection {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private final static int DEFAULT_SERVER_SELECTION_TIMEOUT = 10000;

    protected DBConnection connection;

    public FakedSslConnection(DBConnectionString dbConnectionString) {
        this.connection = new Connection(dbConnectionString,
                new MongoClient(new MongoClientURI(new DBConnectionString.Auth(dbConnectionString).connectionString(),
                        MongoClientOptions.builder().
                                socketFactory(fakeSSlContext().getSocketFactory()).
                                serverSelectionTimeout(DEFAULT_SERVER_SELECTION_TIMEOUT)
                )));
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

    @Override
    public String toString() {
        return connection.toString();
    }

    private SSLContext fakeSSlContext() {
        TrustManager[] noopTrustManager = new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                }
        };
        SSLContext sslContext = null;
        try {
            sslContext = SSLContext.getInstance("ssl");
            sslContext.init(null, noopTrustManager, null);
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        return sslContext;
    }

}
