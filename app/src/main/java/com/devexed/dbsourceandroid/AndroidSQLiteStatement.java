package com.devexed.dbsourceandroid;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.ReadonlyDatabase;
import com.devexed.dbsource.Statement;
import com.devexed.dbsource.Transaction;
import com.devexed.dbsource.util.AbstractCloseable;

abstract class AndroidSQLiteStatement extends AbstractCloseable implements Statement {

    final AndroidSQLiteAbstractDatabase database;
    final Query query;

    AndroidSQLiteStatement(AndroidSQLiteAbstractDatabase database, Query query) {
        this.database = database;
        this.query = query;
    }

    void checkActiveDatabase(ReadonlyDatabase database) {
        if (!(database instanceof AndroidSQLiteAbstractDatabase))
            throw new DatabaseException("Expecting Android SQLite database");

        ((AndroidSQLiteAbstractDatabase) database).checkActive();
    }

    void checkActiveTransaction(Transaction transaction) {
        if (!(transaction instanceof AndroidSQLiteTransaction))
            throw new DatabaseException("Expecting Android SQLite transaction");

        ((AndroidSQLiteTransaction) transaction).checkActive();
    }

}
