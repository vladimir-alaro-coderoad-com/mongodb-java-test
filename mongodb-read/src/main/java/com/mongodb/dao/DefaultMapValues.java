package com.mongodb.dao;

import java.util.Map;

public interface DefaultMapValues {

    public Map _getMapResource();

    public static Boolean _BOOLEAN_DEFAULT_VALUE = Boolean.FALSE;

    public static String _STRING_DEFAULT_VALUE = "";

    public static Integer _INTEGER_DEFAULT_VALUE = 0;

    public static Long _LONG_DEFAULT_VALUE = 0L;

    public static Float _FLOAT_DEFAULT_VALUE = 0F;

    public static Double _DOUBLE_DEFAULT_VALUE = 0.0;

    public void _setMapResource(Map _mapResource);

    default Boolean asBoolean(String key, Boolean defaultValue, Map _mapResource) {
        return _mapResource.containsKey(key) ? Boolean.parseBoolean(_mapResource.get(key).toString()) : defaultValue;
    }

    default Boolean asBoolean(String key, Boolean defaultValue) {
        return asBoolean(key, defaultValue, _getMapResource());
    }

    default Boolean asBoolean(String key) {
        return asBoolean(key, _BOOLEAN_DEFAULT_VALUE, _getMapResource());
    }

    default String asString(String key, String defaultValue, Map _mapResource) {
        return _mapResource.containsKey(key) ? _mapResource.get(key).toString() : defaultValue;
    }

    default String asString(String key, String defaultValue) {
        return asString(key, defaultValue, _getMapResource());
    }

    default String asString(String key) {
        return asString(key, _STRING_DEFAULT_VALUE, _getMapResource());
    }

    default Integer asInteger(String key, Integer defaultValue, Map _mapResource) {
        return _mapResource.containsKey(key) ? Integer.parseInt(_mapResource.get(key).toString()) : defaultValue;
    }

    default Integer asInteger(String key, Integer defaultValue) {
        return asInteger(key, defaultValue, _getMapResource());
    }

    default Integer asInteger(String key) {
        return asInteger(key, _INTEGER_DEFAULT_VALUE, _getMapResource());
    }

    default Long toLong(String key, Long defaultValue, Map _mapResource) {
        return _mapResource.containsKey(key) ? Long.parseLong(_mapResource.get(key).toString()) : defaultValue;
    }

    default Long asLong(String key, Long defaultValue) {
        return toLong(key, defaultValue, _getMapResource());
    }

    default Long asLong(String key) {
        return toLong(key, _LONG_DEFAULT_VALUE, _getMapResource());
    }

    default Float asFloat(String key, Float defaultValue, Map _mapResource) {
        return _mapResource.containsKey(key) ? Float.parseFloat(_mapResource.get(key).toString()) : defaultValue;
    }

    default Float asFloat(String key, Float defaultValue) {
        return asFloat(key, defaultValue, _getMapResource());
    }

    default Float asFloat(String key) {
        return asFloat(key, _FLOAT_DEFAULT_VALUE, _getMapResource());
    }

    default Double asDouble(String key, Double defaultValue, Map _mapResource) {
        return _mapResource.containsKey(key) ? Double.parseDouble(_mapResource.get(key).toString()) : defaultValue;
    }

    default Double asDouble(String key, Double defaultValue) {
        return asDouble(key, defaultValue, _getMapResource());
    }

    default Double asDouble(String key) {
        return asDouble(key, _DOUBLE_DEFAULT_VALUE, _getMapResource());
    }

}
