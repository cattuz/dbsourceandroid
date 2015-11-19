package com.devexed.dbsourceandroid;

/**
 * A factory which creates Android SQLite column and parameter accessors for types.
 */
public interface AndroidSQLiteAccessorFactory {

    AndroidSQLiteAccessor create(Class<?> type);

}
