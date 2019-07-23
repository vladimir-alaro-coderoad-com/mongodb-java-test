package com.mongodb.dao.mongo;

import java.util.logging.Logger;

public class ConnectionAuth {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private String username;
    private String password;
    private String authDatabase;
    private boolean quiet;
    private boolean ssl;

    public ConnectionAuth(String username, String password, String authDatabase, boolean quiet, boolean ssl) {
        this.username = username;
        this.password = password;
        this.authDatabase = authDatabase;
        this.quiet = quiet;
        this.ssl = ssl;
    }

    public String commandAuths() {
        return (!authDatabase.trim().isEmpty() ? "--authenticationDatabase=" + authDatabase : "") +
                (quiet ? " --quiet" : "") + (ssl ? " --ssl" : "") +
                " --username=" + username + " --password=\"" + password + "\"";
    }

    public String urlAuths() {
        return username + ":" + password + "@";
    }

}

