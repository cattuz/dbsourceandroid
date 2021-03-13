package com.devexed.dalwitandroid;

import org.sqlite.database.sqlite.SQLiteStatement;

final class SQLiteStatementBindable implements AndroidSQLiteBindable {

    private final SQLiteStatement statement;

    public SQLiteStatementBindable(SQLiteStatement statement) {
        this.statement = statement;
    }

    @Override
    public void bindNull(int index) {
        statement.bindNull(index + 1);
    }

    @Override
    public void bindLong(int index, long value) {
        statement.bindLong(index + 1, value);
    }

    @Override
    public void bindString(int index, String value) {
        statement.bindString(index + 1, value);
    }

    @Override
    public void bindDouble(int index, double value) {
        statement.bindDouble(index + 1, value);
    }

    @Override
    public void bindBlob(int index, byte[] bytes) {
        statement.bindBlob(index + 1, bytes);
    }

}
