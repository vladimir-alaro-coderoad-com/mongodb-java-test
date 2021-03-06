package com.mongodb.services;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.dao.MongoDAOAsync;
import com.mongodb.util.Constants;
import com.mongodb.util.Pagination;
import com.mongodb.util.PropertiesService;
import org.bson.Document;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

@Dependent
public class AsynchronousDBServices extends OperationForExecutionAsync {

    @Inject
    MongoDAOAsync mongoDAOAsync;
    @Inject
    Logger logger;

    @Inject
    transient PropertiesService propertiesService;

    private MongoCollection getCollection(String collectionName) {
        String dataBaseName = propertiesService.get(Constants.DEFAULT_DATABASE_KEY);
        logger.info("Async connection to dataBase : " + dataBaseName + ", collection : " + collectionName);
        return mongoDAOAsync.collection(dataBaseName, collectionName);
    }

    public List<Map<String, Object>> getDocsWithCommandFind(String collection, JsonObject payload, Pagination pagination) {
        MongoCollection mongoCollection = getCollection(collection);
        List<Map<String, Object>> finalList = Collections.synchronizedList(new ArrayList<>());
        final CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();
        _getDocsWithCommandFind(mongoCollection, payload, pagination, finalList, (result, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(result);
            }
        });
        future.join();
        return finalList;
    }

    public List<Map<String, Object>> getDocsWithCommandAggregate(String collection, JsonObject payload, Pagination pagination) {
        MongoCollection mongoCollection = getCollection(collection);
        List<Map<String, Object>> finalList = Collections.synchronizedList(new ArrayList<>());
        final CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();
        _getDocsWithCommandAggregate(mongoCollection, payload, pagination, finalList, (result, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(result);
            }
        });
        future.join();
        return finalList;
    }

    public Long getTotalDocsCommandFind(String collection, JsonObject payload) {
        MongoCollection mongoCollection = getCollection(collection);
        final CompletableFuture<Long> future = new CompletableFuture<>();
        _getTotalDocsCommandFind(mongoCollection, payload, (result, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(result);
            }
        });
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalArgumentException("Error get total", e);
        }
    }

    public Long getTotalDocsCommandAggregate(String collection, JsonObject payload) {
        MongoCollection mongoCollection = getCollection(collection);
        List<Map<String, Object>> finalList = Collections.synchronizedList(new ArrayList<>());
        final CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();
        _getTotalDocsCommandAggregate(mongoCollection, payload, finalList, (result, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(result);
            }
        });
        future.join();
        return getTotalFromAggregationResult(finalList);
    }

    Long getTotalFromAggregationResult(List<Map<String, Object>> finalList) {
        if (!finalList.isEmpty()) {
            Object totalObject = finalList.get(0).get("total");
            if (totalObject instanceof Integer) {
                return (Long.valueOf(String.valueOf(totalObject)));
            } else if (totalObject instanceof Long) {
                return ((Long) totalObject);
            }
        }
        return 0L;
    }

    public int getTotalFunctions() throws ExecutionException, InterruptedException {
        MongoDatabase database = mongoDAOAsync.db(propertiesService.get(Constants.DEFAULT_DATABASE_KEY));
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        database.runCommand(new Document("eval", "db.system.js.count()"), new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                future.complete(result.getDouble("retval").intValue());
            }
        });
        return future.get();
    }

    public Document runFunction() throws ExecutionException, InterruptedException {
        MongoDatabase database = mongoDAOAsync.db(propertiesService.get(Constants.DEFAULT_DATABASE_KEY));
        final CompletableFuture<Document> future = new CompletableFuture<>();
        database.runCommand(new Document("eval", "test()"), new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                future.complete(result);
            }
        });
        return future.get();
    }
}
