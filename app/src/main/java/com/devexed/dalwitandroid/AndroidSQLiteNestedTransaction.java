package com.devexed.dalwitandroid;

import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;

import com.devexed.dalwit.DatabaseException;

/**
 * A nested transaction.
 * <p/>
 * Android's {@link SQLiteDatabase#beginTransaction()} method of transaction does not work correctly for
 * nested transactions. In android if any one transaction is unsuccessful (calls
 * {@link SQLiteDatabase#endTransaction()} before  {@link SQLiteDatabase#setTransactionSuccessful()} all
 * transactions including the parent are rolled back. Therefore we use SQLite's savepoint mechanism
 * directly.
 */
final class AndroidSQLiteNestedTransaction extends AndroidSQLiteTransaction {

    AndroidSQLiteNestedTransaction(AndroidSQLiteTransaction parent) {
        super(parent);

        try {
            connection.execSQL("SAVEPOINT android_sqlite_transaction");
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    void commitTransaction() {
        connection.execSQL("RELEASE SAVEPOINT android_sqlite_transaction");
    }

    @Override
    void rollbackTransaction() {
        connection.execSQL("ROLLBACK TRANSACTION TO SAVEPOINT android_sqlite_transaction");
    }

}
