package com.tierconnect.riot.api.database.mongo.pipeline;

import org.bson.conversions.Bson;

/**
 * Created by vealaro on 1/30/17.
 */
public interface Pipeline {

    String toString();

    Bson toBson();
}
