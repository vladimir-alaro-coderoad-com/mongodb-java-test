package com.mongodb.dao.base;

public interface DBCredentialsPool {

    DBCredentials credentials(String databasename);

}