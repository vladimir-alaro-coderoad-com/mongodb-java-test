package com.mongodb.util;

import org.bson.Document;

import javax.json.JsonObject;

public class ParseUtils {

    public static Document parseJsonToDocument(JsonObject payload) {
        Document document = new Document();
        if (payload != null && !payload.isEmpty()) {
            document = Document.parse(payload.toString());
        }
        return document;
    }
}
