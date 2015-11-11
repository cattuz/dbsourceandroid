package com.devexed.dbsourceandroid;

import android.database.sqlite.SQLiteQuery;

final class SQLiteQueryBindable implements SQLiteBindable {

    private final SQLiteQuery query;

    public SQLiteQueryBindable(SQLiteQuery query) {
        this.query = query;
    }

    @Override
    public void bindNull(int index) {
        query.bindNull(index);
    }

    @Override
    public void bindLong(int index, long value) {
        query.bindLong(index, value);
    }

    @Override
    public void bindString(int index, String value) {
        query.bindString(index, value);
    }

    @Override
    public void bindDouble(int index, double value) {
        query.bindDouble(index, value);
    }

    @Override
    public void bindBlob(int index, byte[] bytes) {
        query.bindBlob(index, bytes);
    }

}
