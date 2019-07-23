package com.mongodb.services;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.util.Pagination;
import com.mongodb.util.ParseUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.json.JsonObject;
import java.util.*;

class OperationForExecution {

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> _getDocsWithCommandFind(MongoCollection mongoCollection, JsonObject payload, Pagination pagination) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        List<Map<String, Object>> result = new ArrayList<>();
        FindIterable findIterable = mongoCollection.find(filter).skip(pagination.getSkip()).limit(pagination.getLimit());
        for (Document doc : (Iterable<Document>) findIterable) {
            Map<String, Object> map = new LinkedHashMap<>(doc);
            result.add(map);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> _getDocsWithCommandAggregate(MongoCollection mongoCollection, JsonObject payload, Pagination pagination) {
        List<Map<String, Object>> result = new ArrayList<>();
        Document filter = ParseUtils.parseJsonToDocument(payload);
        AggregateIterable aggregateIterable = mongoCollection.aggregate(Arrays.asList(
                Aggregates.match(filter),
                Aggregates.skip(pagination.getSkip()),
                Aggregates.limit(pagination.getLimit()))
        );
        for (Document doc : (Iterable<Document>) aggregateIterable) {
            Map<String, Object> map = new LinkedHashMap<>(doc);
            result.add(map);
        }
        return result;
    }

    Long _getTotalDocsCommandFind(MongoCollection mongoCollection, JsonObject payload) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        return mongoCollection.countDocuments(filter);
    }

    @SuppressWarnings("unchecked")
    Long _getTotalDocsCommandAggregate(MongoCollection mongoCollection, JsonObject payload) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        List<Bson> pipelines = new ArrayList<>();
        pipelines.add(Aggregates.match(filter));
        pipelines.add(Aggregates.group("_id", Accumulators.sum("total", 1)));
        AggregateIterable aggregateIterable = mongoCollection.aggregate(pipelines);
        Long result = 0L;
        for (Document doc : (Iterable<Document>) aggregateIterable) {
            Object totalObject = doc.get("total");
            if (totalObject instanceof Integer) {
                result = Long.valueOf(String.valueOf(totalObject));
            } else {
                result = (Long) totalObject;
            }
        }
        return result;
    }
}
