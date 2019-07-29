package com.tierconnect.riot.api.database.mongo.aggregate;

import com.mongodb.client.model.Aggregates;
import com.tierconnect.riot.api.assertions.Assertions;
import com.tierconnect.riot.api.database.mongo.pipeline.Pipeline;
import com.tierconnect.riot.api.database.mongo.pipeline.PipelineBase;
import org.bson.conversions.Bson;

/**
 * Created by vealaro on 1/30/17.
 */
public class MongoLimit extends PipelineBase implements Pipeline {

    private int value;

    private MongoLimit(int value) {
        this.value = value;
    }

    public static MongoLimit create(int value) {
        Assertions.voidNotNull("limit", value);
        return new MongoLimit(value);
    }

    public Bson toBson() {
        return Aggregates.limit(value);
    }

}
