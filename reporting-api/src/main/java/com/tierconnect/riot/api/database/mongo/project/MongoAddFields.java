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

public class MongoAddFields extends PipelineBase implements Pipeline {

    private static Logger logger = LogManager.getLogger(MongoAddFields.class);

    private Map<String, Object> fieldsMap;

    public MongoAddFields() {
        fieldsMap = new LinkedHashMap<>();
    }

    public void addField(String alias, String key) {
        notNull("alias", alias);
        notNull("key", key);
        fieldsMap.put(alias, key);
    }

    public void addField(String alias, Map value) {
        notNull("alias", alias);
        notNull("value", value);
        fieldsMap.put(alias, value);
    }

    @Override
    public Bson toBson() {
        return new Document("$addFields", fieldsMap);
    }

    @Override
    public String toString() {
        return mapToJson(Collections.singletonMap("$addFields", fieldsMap));
    }

    public String mapToJson(Map map) {
        try {
            return super.mapToJson(map);
        } catch (IOException e) {
            logger.error("Error convert map to json string in MongoAddFields : \n " + map, e);
        }
        return null;
    }

}
