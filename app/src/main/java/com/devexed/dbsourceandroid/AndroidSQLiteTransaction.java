package com.devexed.dbsourceandroid;

import android.database.SQLException;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

abstract class AndroidSQLiteTransaction extends AndroidSQLiteAbstractDatabase implements Transaction {

    private final AndroidSQLiteAbstractDatabase parent;
    private boolean committed = false;

    /**
     * Create a root level transaction. Committing this transaction will
     * update the database.
     */
    AndroidSQLiteTransaction(AndroidSQLiteAbstractDatabase parent) {
        super(parent.connection, parent.accessorFactory);
        this.parent = parent;
        beginTransaction();
    }

    abstract void beginTransaction();

    abstract void commitTransaction();

    abstract void rollbackTransaction();

    private void checkNotCommitted() {
        if (committed) throw new DatabaseException("Already committed");
    }

    void checkActive() {
        checkChildClosed();
        checkNotCommitted();
        checkNotClosed();
    }

    @Override
    public final Transaction transact() {
        checkActive();

        try {
            AndroidSQLiteNestedTransaction transaction = new AndroidSQLiteNestedTransaction(this);
            onOpenChild(transaction);
            return transaction;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public final void commit() {
        checkActive();

        try {
            commitTransaction();
            committed = true;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public final void close() {
        if (isClosed()) return;

        super.close();
        parent.onCloseChild();

        try {
            if (!committed) rollbackTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
