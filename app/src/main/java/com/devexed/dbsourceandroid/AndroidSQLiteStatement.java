package com.devexed.dbsourceandroid;

import com.devexed.dbsource.AbstractCloseable;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.Statement;
import com.devexed.dbsource.Transaction;

abstract class AndroidSQLiteStatement extends AbstractCloseable implements Statement {

    final AndroidSQLiteAbstractDatabase database;
    final Query query;

    AndroidSQLiteStatement(AndroidSQLiteAbstractDatabase database, Query query) {
        this.database = database;
        this.query = query;
    }

    final void checkActiveTransaction(Transaction transaction) {
        if (!(transaction instanceof AndroidSQLiteTransaction))
            throw new DatabaseException("Expecting " + AndroidSQLiteTransaction.class + " not " + transaction.getClass());

        ((AndroidSQLiteTransaction) transaction).checkActive();
    }

}
