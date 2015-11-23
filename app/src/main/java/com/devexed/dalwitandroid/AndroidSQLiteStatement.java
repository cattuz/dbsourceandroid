package com.devexed.dalwitandroid;

import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Query;
import com.devexed.dalwit.ReadonlyDatabase;
import com.devexed.dalwit.Statement;
import com.devexed.dalwit.Transaction;
import com.devexed.dalwit.util.AbstractCloseable;

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
