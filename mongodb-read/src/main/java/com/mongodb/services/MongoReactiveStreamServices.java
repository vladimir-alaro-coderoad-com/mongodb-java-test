package com.mongodb.services;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
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
}
