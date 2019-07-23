package com.mongodb.dao.mongo;

import com.mongodb.dao.DefaultMapValues;
import com.mongodb.util.PropertiesService;

import java.util.Map;

public class MongoDBParameters implements DefaultMapValues {

    private Map mapConfigs;

    public Boolean sharding;
    public String primary;
    public String secondary;

    public String host;
    public Integer port;
    public String database;


    public String username;
    public String password;
    public String authDatabase;
    public Boolean quiet;

    public String replicaSet;
    public String readPreferenceControl;
    public String readPreferenceReports;
    public Integer connectTimeout;
    public Integer socketTimeout;
    public Integer maxPoolSize;
    public Integer minPoolSize;
    public Integer maxIdleTime;
    public Boolean ssl;

    public Boolean isMongoAtlas;
    public String atlasCloud;
    public String atlasUser;
    public String atlasApiKey;

    public MongoDBParameters(Map properties) {

        _setMapResource(properties);

        sharding = asBoolean("mongo.sharding");
        primary = asString("mongo.primary").trim();
        secondary = asString("mongo.secondary").trim();

        host = asString("mongo.host").trim();
        port = asInteger("mongo.port", 27017);
        database = asString("mongo.db").trim();

        username = asString("mongo.username");
        password = asString("mongo.password");
        authDatabase = asString("mongo.authdb");
        quiet = asBoolean("mongo.quiet", true);

        replicaSet = asString("mongo.replicaset").trim();
        readPreferenceControl = asString("mongo.controlReadPreference").trim();
        readPreferenceReports = asString("mongo.reportsReadPreference").trim();
        connectTimeout = asInteger("mongo.connectiontimeout", 3000);
        connectTimeout = asInteger("mongo.sockettimeout", 3000);
        maxPoolSize = asInteger("mongo.maxpoolsize", 100);
        minPoolSize = asInteger("mongo.minpoolsize", 0);
        maxIdleTime = asInteger("mongo.maxidletime", 60000);
        ssl = asBoolean("mongo.ssl");

        isMongoAtlas = asBoolean("mongo.atlasEnvironment");
        atlasCloud = asString("mongo.atlasCloud");
        atlasUser = asString("mongo.atlasUser");
        atlasApiKey = asString("mongo.atlasApiKey");
    }

    public MongoDBParameters(PropertiesService propertiesService) {

        sharding = false;
        primary = propertiesService.get("VIZIX_MONGO_PRIMARY", "");
        secondary = propertiesService.get("VIZIX_MONGO_SECONDARY", "");

        database = propertiesService.get("VIZIX_MONGO_DATABASE", "");
        username = propertiesService.get("VIZIX_MONGO_USERNAME", "");
        password = propertiesService.get("VIZIX_MONGO_PASSWORD", "");
        authDatabase = propertiesService.get("VIZIX_MONGO_AUTHDB", "");
        quiet = true;

        replicaSet = propertiesService.get("VIZIX_MONGO_REPLICASET", "");
        readPreferenceControl = propertiesService.get("VIZIX_MONGO_CONTROL_READPREFERENCE", "");
        readPreferenceReports = propertiesService.get("VIZIX_MONGO_REPORTS_READPREFERENCE", "");
        connectTimeout = Integer.valueOf(propertiesService.get("VIZIX_MONGO_CONNECTION_TIMEOUT", "5000"));
        socketTimeout = Integer.valueOf(propertiesService.get("VIZIX_MONGO_SOCKET_TIMEOUT", "30000"));
        maxPoolSize = Integer.valueOf(propertiesService.get("VIZIX_MONGO_MAX_POOL_SIZE", "100"));
        minPoolSize = Integer.valueOf(propertiesService.get("VIZIX_MONGO_MIN_POOL_SIZE", "0"));
        maxIdleTime = Integer.valueOf(propertiesService.get("VIZIX_MONGO_MAX_IDLE_TIME", "60000"));
        ssl = "true".equalsIgnoreCase(propertiesService.get("VIZIX_MONGO_SSL", ""));

        isMongoAtlas = false;
        atlasCloud = "";
        atlasUser = "";
        atlasApiKey = "";
    }

    @Override
    public Map _getMapResource() {
        return mapConfigs;
    }

    @Override
    public void _setMapResource(Map _mapResource) {
        mapConfigs = _mapResource;
    }

    @Override
    public String toString() {
        return "\n - mapConfigs; " + mapConfigs +
                "\n - sharding: " + sharding +
                "\n - primary: " + primary +
                "\n - secondary: " + secondary +
                "\n - host: " + host +
                "\n - port: " + port +
                "\n - database: " + database +
                "\n - authDatabase: " + authDatabase +
                "\n - quiet: " + quiet +
                "\n - replicaSet: " + replicaSet +
                "\n - readPreferenceControl: " + readPreferenceControl +
                "\n - readPreferenceReports: " + readPreferenceReports +
                "\n - connectTimeout: " + connectTimeout +
                "\n - maxPoolSize: " + maxPoolSize +
                "\n - ssl: " + ssl;
    }

}
