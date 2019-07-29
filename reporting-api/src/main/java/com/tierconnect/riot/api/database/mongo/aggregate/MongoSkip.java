package com.tierconnect.riot.api.database.mongo.aggregate;

import com.mongodb.client.model.Aggregates;
import com.tierconnect.riot.api.assertions.Assertions;
import com.tierconnect.riot.api.database.mongo.pipeline.Pipeline;
import com.tierconnect.riot.api.database.mongo.pipeline.PipelineBase;
import org.bson.conversions.Bson;

public class MongoSkip extends PipelineBase implements Pipeline {

    private int value;

    private MongoSkip(int value) {
        this.value = value;
    }

    public static MongoSkip create(int value) {
        Assertions.voidNotNull("skip", value);
        return new MongoSkip(value);
    }

    @Override
    public Bson toBson() {
        return Aggregates.skip(value);
    }
}
