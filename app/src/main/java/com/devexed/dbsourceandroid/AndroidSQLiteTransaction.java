package com.devexed.dbsourceandroid;

import android.database.SQLException;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

final class AndroidSQLiteTransaction extends AndroidSQLiteAbstractDatabase implements Transaction {

	private boolean committed = false;
    private boolean hasChild = false;
	
	/**
	 * Create a root level transaction. Committing this transaction will
	 * update the database.
	 */
	AndroidSQLiteTransaction(AndroidSQLiteAbstractDatabase parent) {
		super(parent.connection, parent.accessors);
	}

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
            return new AndroidSQLiteTransaction(this);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public final void commit() {
        checkActive();

        try {
            connection.setTransactionSuccessful();
            committed = true;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public final void close() {
        checkNotClosed();
        checkChildClosed();

        try {
            connection.endTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        super.close();
    }
	
}
