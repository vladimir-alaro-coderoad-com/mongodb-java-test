package com.mongodb.dao.base;

public interface ShellDataAccess {

    String shellConnection();

    String shellConnection(String databaseName);

}