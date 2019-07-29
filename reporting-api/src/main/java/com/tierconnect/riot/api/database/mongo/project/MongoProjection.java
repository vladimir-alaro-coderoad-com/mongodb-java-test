package com.tierconnect.riot.api.database.mongo.project;

import com.tierconnect.riot.api.database.mongo.pipeline.Pipeline;
import com.tierconnect.riot.api.database.mongo.pipeline.PipelineBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.tierconnect.riot.api.assertions.Assertions.notNull;

public class MongoProjection extends PipelineBase implements Pipeline {

    private static Logger logger = LogManager.getLogger(MongoProjection.class);

    private Map<String, Object> projectionMap;

    public MongoProjection() {
        projectionMap = new LinkedHashMap<>();
    }

    public void addProjection(String alias, String key) {
        notNull("alias", alias);
        notNull("key", key);
        projectionMap.put(alias, key);
    }

    public void addProjection(String key) {
        notNull("key", key);
        projectionMap.put(key, 1);
    }

    public void excludeProjection(String key) {
        notNull("key", key);
        projectionMap.put(key, 0);
    }

    public void addProjection(String alias, Map value) {
        notNull("alias", alias);
        notNull("value", value);
        projectionMap.put(alias, value);
    }

    public Map<String, Object> getProjectionMap() {
        return projectionMap;
    }

    public boolean isEmpty() {
        return projectionMap.isEmpty();
    }

    @Override
    public Bson toBson() {
        return new Document("$project", projectionMap);
    }

    @Override
    public String toString() {
        return mapToJson(Collections.singletonMap("$project", projectionMap));
    }

    public String mapToJson(Map map) {
        try {
            return super.mapToJson(map);
        } catch (IOException e) {
            logger.error("Error convert map to json string in Mongo projection : \n " + map, e);
        }
        return null;
    }
}
