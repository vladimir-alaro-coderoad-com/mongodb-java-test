package com.tierconnect.riot.api.database.base.ordination;

/**
 * Project: riot-core-services
 * Author: edwin
 * Date: 03/12/2016 - 09:42 AM
 */
public enum Order {
    ASC, DESC;

    public static boolean isValid(String value) {
        return ASC.toString().equals(value) || DESC.toString().equals(value);
    }
}
