package com.tierconnect.riot.api.database.mongo.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Created by vealaro on 1/30/17.
 */
public abstract class PipelineBase {

    protected static final String LIMIT = "$limit";
    protected static final String GROUP = "\"$group\"";
    protected static final String MATCH = "\"$match\"";

    public abstract Bson toBson();

    @Override
    public String toString() {
        Bson bson = toBson();
        if (bson == null) return "null";
        return bson.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry()).toJson();
    }

    public String mapToJson(@SuppressWarnings("rawtypes") Map map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        mapper.writerWithDefaultPrettyPrinter().writeValue(b, map);
        return b.toString("UTF-8");
    }
}
