package com.mongodb.services;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.util.Pagination;
import com.mongodb.util.ParseUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.json.JsonObject;
import java.util.*;

public class OperationForExecutionAsync {

    @SuppressWarnings("unchecked")
    void _getDocsWithCommandFind(MongoCollection mongoCollection,     // IN
                                 JsonObject payload,                  // IN
                                 Pagination pagination,               // IN
                                 List<Map<String, Object>> finalList, // OUT
                                 final SingleResultCallback<List<Map<String, Object>>> callbackWhenFinished
    ) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        mongoCollection.find(filter).skip(pagination.getSkip()).limit(pagination.getLimit())
                .map(this::documentToMap)
                .into(finalList, callbackWhenFinished);
    }

    @SuppressWarnings("unchecked")
    void _getDocsWithCommandAggregate(MongoCollection mongoCollection,
                                      JsonObject payload,
                                      Pagination pagination,
                                      List<Map<String, Object>> finalList,
                                      final SingleResultCallback<List<Map<String, Object>>> callbackWhenFinished) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        List<Bson> pipelines = Arrays.asList(
                Aggregates.match(filter),
                Aggregates.skip(pagination.getSkip()),
                Aggregates.limit(pagination.getLimit())
        );
        _getDocsWithCommandAggregate(mongoCollection, finalList, pipelines, callbackWhenFinished);
    }

    void _getTotalDocsCommandFind(MongoCollection mongoCollection,
                                  JsonObject payload,
                                  final SingleResultCallback<Long> callbackWhenFinished) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        mongoCollection.countDocuments(filter, callbackWhenFinished);
    }

    void _getTotalDocsCommandAggregate(MongoCollection mongoCollection,
                                       JsonObject payload,
                                       List<Map<String, Object>> finalList,
                                       final SingleResultCallback<List<Map<String, Object>>> callbackWhenFinished
    ) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        List<Bson> pipelines = new ArrayList<>();
        pipelines.add(Aggregates.match(filter));
        pipelines.add(Aggregates.group("_id", Accumulators.sum("total", 1)));
        _getDocsWithCommandAggregate(mongoCollection, finalList, pipelines, callbackWhenFinished);
    }

    @SuppressWarnings("unchecked")
    private void _getDocsWithCommandAggregate(MongoCollection mongoCollection,
                                              List<Map<String, Object>> finalList,
                                              List<Bson> pipelines,
                                              final SingleResultCallback<List<Map<String, Object>>> callbackWhenFinished) {
        mongoCollection.aggregate(pipelines)
                .map(doc -> new HashMap<String, Object>((Map<? extends String, Object>) doc))
                .into(finalList, callbackWhenFinished);
    }

    @SuppressWarnings("unchecked")
    private HashMap documentToMap(Object document) {
        if (Objects.isNull(document)) return null;
        return new HashMap<>((Map) document);
    }
}
