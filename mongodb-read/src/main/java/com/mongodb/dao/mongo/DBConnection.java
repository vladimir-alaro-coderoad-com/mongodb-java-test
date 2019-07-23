package com.mongodb.dao.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public interface DBConnection {

    MongoClient mongoClient();

    MongoDatabase mongoDatabase(String databaseName);

    MongoCollection mongoCollection(String databaseName, String collectionName);

}

