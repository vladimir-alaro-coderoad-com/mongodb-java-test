package com.tierconnect.riot.api.database.mongo.aggregate;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import com.tierconnect.riot.api.database.base.ordination.Order;
import com.tierconnect.riot.api.database.mongo.pipeline.Pipeline;
import com.tierconnect.riot.api.database.mongo.pipeline.PipelineBase;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.tierconnect.riot.api.assertions.Assertions.notNull;

public class MongoSort extends PipelineBase implements Pipeline {

    private Map<String, Order> sortMap;

    public MongoSort() {
        sortMap = new LinkedHashMap<>();
    }

    public void addSort(String property, Order order) {
        notNull("property", property);
        notNull("order", order);
        sortMap.put(property, order);
    }

    public void addSort(Map<String, Order> sortMapIn) {
        notNull("map", sortMapIn);
        sortMap.putAll(sortMapIn);
    }

    public void clearMap() {
        sortMap.clear();
    }

    public boolean isEmpty(){
        return sortMap.isEmpty();
    }

    @Override
    public Bson toBson() {
        List<Bson> sortList = new ArrayList<>();
        for (Map.Entry<String, Order> map : sortMap.entrySet()) {
            if (Order.DESC.equals(map.getValue())) {
                sortList.add(Sorts.descending(map.getKey()));
            } else {
                sortList.add(Sorts.ascending(map.getKey()));
            }
        }
        return Aggregates.sort(Sorts.orderBy(sortList));
    }
}
