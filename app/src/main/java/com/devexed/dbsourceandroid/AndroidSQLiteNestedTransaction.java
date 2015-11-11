package com.devexed.dbsourceandroid;

import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

/**
 * A nested transaction.
 *
 * Android's {@link SQLiteDatabase#beginTransaction()} method of transaction does not work correctly for
 * nested transactions. In android if any one transaction is unsuccessful (calls
 * {@link SQLiteDatabase#endTransaction()} before  {@link SQLiteDatabase#setTransactionSuccessful()} all
 * transactions including the parent are rolled back. Therefore we use SQLite's savepoint mechanism
 * directly.
 */
final class AndroidSQLiteNestedTransaction extends AndroidSQLiteTransaction {

    private final AndroidSQLiteTransaction parent;

	AndroidSQLiteNestedTransaction(AndroidSQLiteTransaction parent) {
		super(parent);
        this.parent = parent;
	}

    @Override
    void beginTransaction() {
        try {
            connection.execSQL("SAVEPOINT android_sqlite_transaction");
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    void commitTransaction() {
        try {
            connection.execSQL("RELEASE SAVEPOINT android_sqlite_transaction");
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    void rollbackTransaction() {
        try {
            connection.execSQL("ROLLBACK TRANSACTION TO SAVEPOINT android_sqlite_transaction");
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close() {
        parent.closeActiveTransaction();
        super.close();
    }

}
