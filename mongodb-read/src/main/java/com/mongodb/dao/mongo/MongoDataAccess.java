package com.mongodb.dao.mongo;

import com.mongodb.dao.base.ShellDataAccess;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public interface MongoDataAccess extends ShellDataAccess {

    MongoClient client();

    MongoDatabase database(String databaseName);

    MongoCollection<Document> collection(String databaseName, String collectionName);

}
