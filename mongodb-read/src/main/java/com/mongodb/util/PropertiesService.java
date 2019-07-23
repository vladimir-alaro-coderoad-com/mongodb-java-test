package com.mongodb.util;

import javax.cache.Cache;
import javax.cache.annotation.CacheDefaults;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
@CacheDefaults(cacheName = "configuration")
public class PropertiesService {

    @Inject
    Cache<String, String> properties;

    public void put(String key, String value) {
        properties.put(key, value);
    }

    public String get(String key) {
        return properties.get(key);
    }

    public String get(String key, String defaultValue) {
        String val = get(key);
        return val != null ? val : defaultValue;
    }
}
