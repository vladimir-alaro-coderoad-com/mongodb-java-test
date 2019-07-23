package com.mongodb.dao.mongo;

import java.util.ArrayList;

public class ConnectionPoolOptions {

    private int maxPoolSize;
    private int minPoolSize;
    private int maxIdleTimeMS;

    public ConnectionPoolOptions(int maxPoolSize, int minPoolSize, int maxIdleTimeMS) {
        this.maxPoolSize = maxPoolSize;
        this.minPoolSize = minPoolSize;
        this.maxIdleTimeMS = maxIdleTimeMS;
    }

    @Override
    public String toString() {
        ArrayList options = new ArrayList();
        options.add("maxPoolSize=" + maxPoolSize);
        options.add("minPoolSize=" + minPoolSize);
        options.add("maxIdleTimeMS=" + maxIdleTimeMS);
        return String.join("&", options);
    }

}