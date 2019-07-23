package com.mongodb.dao.mongo;

import com.mongodb.dao.base.DBConnectionString;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Connection implements DBConnection {

    protected DBConnectionString connectionString;
    protected MongoClient mongoClient;

    public Connection(DBConnectionString connectionString, MongoClient mongoClient) {
        this.connectionString = connectionString;
        this.mongoClient = mongoClient;
    }

    public Connection(DBConnectionString connectionString) {
        this(connectionString,
                new MongoClient(new MongoClientURI(new DBConnectionString.Auth(connectionString).connectionString())));
    }

    @Override
    public MongoClient mongoClient() {
        return mongoClient;
    }

    @Override
    public MongoDatabase mongoDatabase(String databaseName) {
        return mongoClient().getDatabase(databaseName);
    }

    @Override
    public MongoCollection mongoCollection(String databaseName, String collectionName) {
        return mongoDatabase(databaseName).getCollection(collectionName);
    }

    @Override
    public String toString() {
        return new DBConnectionString.Auth(connectionString).connectionString();
    }

}
