package com.devexed.dalwitandroid;

import org.sqlite.database.SQLException;
import com.devexed.dalwit.*;

import java.util.Map;

abstract class AndroidSQLiteTransaction extends AndroidSQLiteAbstractDatabase implements Transaction {

    private final AndroidSQLiteAbstractDatabase parent;
    private boolean committed = false;

    /**
     * Create a root level transaction. Committing this transaction will
     * update the database.
     */
    AndroidSQLiteTransaction(AndroidSQLiteAbstractDatabase parent) {
        super(parent.connection, parent.accessorFactory, parent.columnNameMapper);
        this.parent = parent;
    }

    @Override
    void checkActive() {
        super.checkActive();
        parent.checkIsChildTransaction(this);
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

        try {
            commitTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        committed = true;
    }

    @Override
    final void closeResource() {
        checkActive();

        if (!committed) {
            try {
                rollbackTransaction();
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        parent.closeChildTransaction(this);
    }

}
