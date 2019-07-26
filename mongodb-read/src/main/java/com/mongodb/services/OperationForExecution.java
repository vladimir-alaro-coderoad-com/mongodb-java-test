package com.mongodb.services;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.*;
import com.mongodb.util.Constants;
import com.mongodb.util.Pagination;
import com.mongodb.util.ParseUtils;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.json.JsonObject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> _getDocsWithCommandFindWithSubQueryOP1(MongoCollection mongoCollectionHistory,
                                                                     MongoCollection mongoCollectionIndex,
                                                                     JsonObject payload, Pagination pagination) {
        List<Object> listId = new ArrayList<>();
        Document mainFilter = ParseUtils.parseJsonToDocument(payload);
        MongoIterable<Document> mongoIterable = getResultSubQuery(mongoCollectionIndex, null, null, 0, 0);
        mongoIterable.map(doc -> doc.get(Constants.OID)).into(listId);
        printQuery("FIND", mainFilter);

        List<Bson> finalFilter = new ArrayList<>();
        if (!mainFilter.isEmpty()) {
            finalFilter.add(mainFilter);
        }
        finalFilter.add(Filters.in(Constants._ID, listId));

        List<Map<String, Object>> result = new ArrayList<>();
        mongoCollectionHistory.find(Filters.and(finalFilter)).skip(pagination.getSkip()).limit(pagination.getLimit()).into(result);
        return result;
    }

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> _getDocsWithCommandFindWithSubQueryOP2(MongoCollection mongoCollectionHistory,
                                                                     MongoCollection mongoCollectionIndex,
                                                                     JsonObject payload, Pagination pagination) {
        Document mainFilter = ParseUtils.parseJsonToDocument(payload);
        MongoIterable<Document> mongoIterable = getResultSubQuery(mongoCollectionIndex, null, null, 0, 0);
        Iterable<Object> resultSubQuery = mongoIterable
                .map(doc -> doc.get(Constants.OID));
        printQuery("FIND", mainFilter);
        List<Bson> finalFilter = new ArrayList<>();
        if (!mainFilter.isEmpty()) {
            finalFilter.add(mainFilter);
        }
        finalFilter.add(Filters.in(Constants._ID, resultSubQuery));

        List<Map<String, Object>> result = new ArrayList<>();
        mongoCollectionHistory.find(Filters.and(finalFilter)).skip(pagination.getSkip()).limit(pagination.getLimit()).into(result);
        return result;
    }

    private MongoIterable executeSubQuery(MongoCollection mongoCollection) {
        return mongoCollection.find().projection(Projections.include("_id"));
    }

    private Long getSize(Iterable iterable) {
        return StreamSupport.stream(iterable.spliterator(), Boolean.FALSE).count();
    }

    @SuppressWarnings("unchecked")
    private List<Bson> getListFilter(MongoCollection mongoCollection) {
        Iterable<Document> mongoIterable = executeSubQuery(mongoCollection);
        final int chunkSize = 400000;
        final AtomicInteger counter = new AtomicInteger();
        Collection<List<Object>> listCollection = StreamSupport.stream(mongoIterable.spliterator(), Boolean.FALSE)
                .map(document -> document.get("_id"))
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize)).values();
        return listCollection.stream().
                map(it -> Filters.in("_id", it))
                .collect(Collectors.toList());
    }

    private Bson mergeFilter(Document mainFilter, Bson subQuery) {
        final List<Bson> finalFilter = new ArrayList<>();
        finalFilter.add(subQuery);
        if (!mainFilter.isEmpty()) {
            finalFilter.add(mainFilter);
        }
        if (finalFilter.isEmpty()) {
            return new Document();
        }
        return Filters.and(finalFilter);
    }

    private void printQuery(String prefix, Bson filter) {
        BsonDocument bsonDocument = filter.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
        System.out.println("\n" + prefix + "\n" + bsonDocument.toJson() + "\n\n");
    }

    private MongoIterable<Document> getResultSubQuery(MongoCollection<Document> mongoCollection, Long startDate, Long endDate, int skip, int limit) {
        List<Bson> pipelines = executeThingSnapshotsIndex(startDate, endDate, skip, limit);
        return mongoCollection.aggregate(pipelines).allowDiskUse(Boolean.TRUE);
    }

    private List<Bson> executeThingSnapshotsIndex(Long startDate, Long endDate, int skip, int limit) {
        List<Bson> pipelines = new ArrayList<>();

        Bson match = null;
        if (Objects.nonNull(startDate) && Objects.nonNull(endDate)) {
            match = Aggregates.match(Filters.and(Filters.gte(Constants.TIME, new Date(startDate)), Filters.lte(Constants.TIME, new Date(endDate))));
        } else if (Objects.nonNull(endDate)) {
            match = Aggregates.match(Filters.lte(Constants.TIME, new Date(endDate)));
        } else if (Objects.nonNull(startDate)) {
            match = Aggregates.match(Filters.gte(Constants.TIME, new Date(startDate)));
        }
        if (Objects.nonNull(match)) {
            pipelines.add(Aggregates.match(match));
        }
        pipelines.add(Aggregates.sort(Sorts.descending(Constants.TIME)));
        pipelines.add(Aggregates.group(_$(Constants.THING_ID), Accumulators.first(Constants.OID, _$(Constants._ID))));
        if (skip != 0) {
            pipelines.add(Aggregates.skip(0));
        }
        if (limit != 0) {
            pipelines.add(Aggregates.limit(0));
        }
//        printQuery("", new Document("AGGREGATE", pipelines));
        return pipelines;
    }

    private String _$(String value) {
        return "$" + value;
    }

}
