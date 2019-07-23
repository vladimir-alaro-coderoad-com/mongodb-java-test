package com.mongodb.services;

import com.mongodb.client.MongoCollection;
import com.mongodb.dao.MongoDAO;
import com.mongodb.util.Constants;
import com.mongodb.util.PropertiesService;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Dependent
public class SynchronousDBServices extends OperationForExecution {

    @Inject
    MongoDAO mongoDAO;
    @Inject
    Logger logger;
    @Inject
    transient PropertiesService propertiesService;

    private MongoCollection getCollection(String collectionName) {
        String dataBaseName = propertiesService.get(Constants.DEFAULT_DATABASE_KEY);
        logger.info("dataBase : " + dataBaseName + ", collection : " + collectionName);
        return mongoDAO.collection(dataBaseName, collectionName);
    }

    public List<Map<String, Object>> getDocsWithCommandFind(String collection, JsonObject payload) {
        MongoCollection mongoCollection = getCollection(collection);
        return _getDocsWithCommandFind(mongoCollection, payload);
    }

    public List<Map<String, Object>> getDocsWithCommandAggregate(String collection, JsonObject payload) {
        MongoCollection mongoCollection = getCollection(collection);
        return _getDocsWithCommandAggregate(mongoCollection, payload);
    }

    public Long getTotalDocsCommandFind(String collection, JsonObject payload) {
        MongoCollection mongoCollection = getCollection(collection);
        return _getTotalDocsCommandFind(mongoCollection, payload);
    }

    public Long getTotalDocsCommandAggregate(String collection, JsonObject payload) {
        MongoCollection mongoCollection = getCollection(collection);
        return _getTotalDocsCommandAggregate(mongoCollection, payload);
    }
}
