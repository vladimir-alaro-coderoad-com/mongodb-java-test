package com.mongodb.util;

import com.mongodb.MongoClient;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.json.JsonObject;

public class ParseUtils {

    public static Document parseJsonToDocument(JsonObject payload) {
        Document document = new Document();
        if (payload != null && !payload.isEmpty()) {
            document = Document.parse(payload.toString());
        }
        return document;
    }

    public static String _$(String value) {
        return "$" + value;
    }

    public static void printQuery(String prefix, Bson filter) {
        BsonDocument bsonDocument = filter.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
        System.out.println("\n" + prefix + "\n" + bsonDocument.toJson() + "\n\n");
    }
}
