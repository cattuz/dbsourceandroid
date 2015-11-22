package com.devexed.dbsourceandroid;

import com.devexed.dbsource.Transaction;

abstract class AndroidSQLiteTransaction extends AndroidSQLiteAbstractDatabase implements Transaction {

    /**
     * Create a root level transaction. Committing this transaction will
     * update the database.
     */
    AndroidSQLiteTransaction(AndroidSQLiteAbstractDatabase parent) {
        super(Transaction.class, parent.connection, parent.accessorFactory);
    }

    abstract void commitTransaction();

    abstract void rollbackTransaction();

    @Override
    public final Transaction transact() {
        checkActive();
        return openTransaction(new AndroidSQLiteNestedTransaction(this));
    }

}
