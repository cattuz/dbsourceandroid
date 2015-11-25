package com.devexed.dalwitandroid;

import android.database.SQLException;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Transaction;

abstract class AndroidSQLiteTransaction extends AndroidSQLiteAbstractDatabase implements Transaction {

    private final AndroidSQLiteAbstractDatabase parent;
    private boolean committed = false;

    /**
     * Create a root level transaction. Committing this transaction will
     * update the database.
     */
    AndroidSQLiteTransaction(AndroidSQLiteAbstractDatabase parent) {
        super("transaction", parent.connection, parent.accessorFactory);
        this.parent = parent;
    }

    abstract void commitTransaction();

    abstract void rollbackTransaction();

    @Override
    public final Transaction transact() {
        checkActive();
        return openChildTransaction(new AndroidSQLiteNestedTransaction(this));
    }

    @Override
    public final void commit() {
        checkActive();
        parent.checkChildTransaction(this);

        try {
            commitTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        committed = true;
    }

    @Override
    public final void close() {
        if (isClosed()) return;

        checkActive();
        parent.checkChildTransaction(this);

        if (!committed) {
            try {
                rollbackTransaction();
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        parent.closeChildTransaction(this);
        super.close();
    }

}
