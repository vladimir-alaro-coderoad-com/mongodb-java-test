package com.mongodb.dao.base;

import com.mongodb.dao.mongo.ConnectionOptions;

public interface DBConnectionString {

    String connectionString(boolean withAuthorization);

    ConnectionOptions options();

    final class Auth {

        private final DBConnectionString connectionString;

        public Auth(DBConnectionString connectionString) {
            this.connectionString = connectionString;
        }

        public String connectionString() {
            return connectionString.connectionString(true);
        }

    }

}
