package com.devexed.dbsourceandroid;

import android.database.SQLException;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

abstract class AndroidSQLiteTransaction extends AndroidSQLiteAbstractDatabase implements Transaction {

	private boolean committed = false;
    private boolean hasChild = false;
	
	/**
	 * Create a root level transaction. Committing this transaction will
	 * update the database.
	 */
	AndroidSQLiteTransaction(AndroidSQLiteAbstractDatabase parent) {
		super(parent.connection, parent.accessors);
        beginTransaction();
	}

    abstract void beginTransaction();

    abstract void commitTransaction();

    abstract void rollbackTransaction();

    private void checkNotCommitted() {
        if (committed) throw new DatabaseException("Already committed");
    }

    private void checkChildClosed() {
        if (hasChild) throw new DatabaseException("Transaction has an open child transaction.");
    }

	void checkActive() {
        checkChildClosed();
		checkNotCommitted();
		checkNotClosed();
	}

    void closeActiveTransaction() {
        if (!hasChild) throw new DatabaseException("Child transaction already closed.");
        hasChild = false;
    }

    @Override
    public final Transaction transact() {
        checkActive();

        try {
            hasChild = true;
            return new AndroidSQLiteNestedTransaction(this);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public final void commit() {
        checkActive();
        commitTransaction();
        committed = true;
    }

    @Override
    public void close() {
        checkNotClosed();
        checkChildClosed();
        if (!committed) rollbackTransaction();
        super.close();
    }
	
}
