package com.mongodb.dao.mongo;

import com.mongodb.dao.base.DBConnectionString;

import java.util.logging.Level;
import java.util.logging.Logger;

public class NetworkConnectionString implements DBConnectionString {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private ConnectionTarget connectionTarget;
    private ConnectionAuth connectionAuth;
    private ConnectionOptions connectionOptions;
    private ConnectionPoolOptions connectionPoolOptions;


    public NetworkConnectionString(ConnectionTarget connectionTarget, ConnectionAuth connectionAuth,
                                   ConnectionOptions connectionOptions, ConnectionPoolOptions connectionPoolOptions) {
        this.connectionTarget = connectionTarget;
        this.connectionAuth = connectionAuth;
        this.connectionOptions = connectionOptions;
        this.connectionPoolOptions = connectionPoolOptions;
    }

    @Override
    public String connectionString(boolean withAuthorization) {
        String target = connectionTarget.target();
        String connectionStr = "mongodb://" +
                (withAuthorization && connectionAuth != null ? connectionAuth.urlAuths() : "") +
                target + "?" + (connectionOptions != null ? connectionOptions.urlOptions() : "") +
                (connectionPoolOptions != null ? "&" + connectionPoolOptions : "");
        logger.log(Level.CONFIG, "Created NetworkConnection to: " + target);
        return connectionStr;
    }

    @Override
    public ConnectionOptions options() {
        return connectionOptions;
    }

}

