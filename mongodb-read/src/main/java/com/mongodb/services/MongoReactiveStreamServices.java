package com.mongodb.services;

import com.mongodb.client.model.*;
import com.mongodb.dao.MongoReactiveStream;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.services.Observables.ListMapsSubscriber;
import com.mongodb.services.Observables.SingleResultSubscriber;
import com.mongodb.util.Constants;
import com.mongodb.util.Pagination;
import com.mongodb.util.ParseUtils;
import com.mongodb.util.PropertiesService;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.mongodb.util.ParseUtils._$;
import static com.mongodb.util.ParseUtils.printQuery;

@SuppressWarnings("unchecked")
@Dependent
public class MongoReactiveStreamServices {

    @Inject
    MongoReactiveStream mongoReactiveStream;

    @Inject
    Logger logger;

    @Inject
    transient PropertiesService propertiesService;

    private MongoCollection getCollection(String collectionName) {
        String dataBaseName = propertiesService.get(Constants.DEFAULT_DATABASE_KEY);
        logger.info("dataBase : " + dataBaseName + ", collection : " + collectionName);
        return mongoReactiveStream.collection(dataBaseName, collectionName);
    }

    public List<Map<String, Object>> getDocsWithCommandFind(String collection, JsonObject payload, Pagination pagination) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        MongoCollection mongoCollection = getCollection(collection);
        ListMapsSubscriber subscriber = new ListMapsSubscriber();
        mongoCollection.find(filter).skip(pagination.getSkip()).limit(pagination.getLimit()).subscribe(subscriber);
        try {
            subscriber.await();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return subscriber.getResults();
    }

    public List<Map<String, Object>> getDocsWithCommandAggregate(String collection, JsonObject payload, Pagination pagination) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        MongoCollection mongoCollection = getCollection(collection);
        ListMapsSubscriber subscriber = new ListMapsSubscriber();
        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(filter),
                Aggregates.skip(pagination.getSkip()),
                Aggregates.limit(pagination.getLimit())
        );
        mongoCollection.aggregate(pipeline).subscribe(subscriber);
        try {
            subscriber.await();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return subscriber.getResults();
    }

    public Long getTotalDocsCommandFind(String collection, JsonObject payload) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        MongoCollection mongoCollection = getCollection(collection);
        SingleResultSubscriber<Long> subscriber = new SingleResultSubscriber<>(0L);
        mongoCollection.countDocuments(filter).subscribe(subscriber);
        try {
            subscriber.await();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return subscriber.getResult();
    }


    public Long getTotalDocsCommandAggregate(String collection, JsonObject payload) {
        Document filter = ParseUtils.parseJsonToDocument(payload);
        MongoCollection mongoCollection = getCollection(collection);
        ListMapsSubscriber subscriber = new ListMapsSubscriber();
        List<Bson> pipelines = new ArrayList<>();
        pipelines.add(Aggregates.match(filter));
        pipelines.add(Aggregates.group("_id", Accumulators.sum("total", 1)));
        mongoCollection.aggregate(pipelines).subscribe(subscriber);
        try {
            subscriber.await();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        if (!subscriber.getResults().isEmpty()) {
            Object totalObject = subscriber.getResults().get(0).get("total");
            if (totalObject instanceof Integer) {
                return (Long.valueOf(String.valueOf(totalObject)));
            } else if (totalObject instanceof Long) {
                return ((Long) totalObject);
            }
        }
        return 0L;
    }


    public List<Map<String, Object>> getDocsWithCommandFindWithSubQueryOP1(JsonObject payload, Pagination pagination) {
        MongoCollection mongoCollectionHistory = getCollection(Constants.HISTORY_COLLECTION);
        MongoCollection mongoCollectionIndex = getCollection(Constants.INDEX_COLLECTION);

        Document mainFilter = ParseUtils.parseJsonToDocument(payload);
        List<Map<String, Object>> listId = getResultSubQuery(mongoCollectionIndex, null, null, 0, 0);
        printQuery("FIND", mainFilter);

        List<Bson> finalFilter = new ArrayList<>();
        if (!mainFilter.isEmpty()) {
            finalFilter.add(mainFilter);
        }
        finalFilter.add(Filters.in(Constants._ID,
                listId.stream().map(doc -> doc.get(Constants.OID)).collect(Collectors.toList())));


        ListMapsSubscriber subscriber = new ListMapsSubscriber();
        mongoCollectionHistory.find(Filters.and(finalFilter)).skip(pagination.getSkip()).limit(pagination.getLimit()).subscribe(subscriber);

        try {
            subscriber.await();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return subscriber.getResults();
    }

    public List<Map<String, Object>> getDocsWithCommandFindWithSubQueryOP2(JsonObject payload, Pagination pagination) {
        MongoCollection mongoCollectionHistory = getCollection(Constants.HISTORY_COLLECTION);
        Document mainFilter = ParseUtils.parseJsonToDocument(payload);

        List<Bson> pipelines = new ArrayList<>();
        if (!mainFilter.isEmpty()) {
            pipelines.add(Aggregates.match(mainFilter));
        }
        pipelines.add(Aggregates.sort(Sorts.descending(Constants.TIME)));
        pipelines.add(Aggregates.group(_$(Constants.VALUE_ID), Accumulators.first(Constants.OID, _$(Constants.VALUE))));
//        pipelines.add(Aggregates.project(Projections.include(Constants.OID)));

        ListMapsSubscriber subscriber = new ListMapsSubscriber();
        mongoCollectionHistory.aggregate(pipelines).allowDiskUse(Boolean.TRUE).subscribe(subscriber);
        try {
            subscriber.await();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return subscriber.getResults();
    }


    private List<Map<String, Object>> getResultSubQuery(MongoCollection mongoCollection, Long startDate, Long endDate, int skip, int limit) {
        List<Bson> pipelines = OperationForExecution.executeThingSnapshotsIndex(startDate, endDate, skip, limit);
        ListMapsSubscriber subscriber = new ListMapsSubscriber();
        mongoCollection.aggregate(pipelines).allowDiskUse(Boolean.TRUE).subscribe(subscriber);
        try {
            subscriber.await();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return subscriber.getResults();
    }

}
