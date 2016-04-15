package com.devexed.dalwitandroid;

import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import com.devexed.dalwit.DatabaseException;

/**
 * A nested transaction.
 * <p/>
 * Android's {@link SQLiteDatabase#beginTransaction()} method of transaction does not work correctly for
 * nested transactions. In Android if any one transaction is unsuccessful (calls
 * {@link SQLiteDatabase#endTransaction()} before  {@link SQLiteDatabase#setTransactionSuccessful()} all
 * transactions including the parent are rolled back. Therefore we use SQLite's savepoint mechanism
 * directly.
 */
class AndroidSQLiteNestedTransaction extends AndroidSQLiteTransaction {

    // Semi-colons required to circumvent Android guessing the wrong type for the statements and therefore not
    // executing them correctly. Who knows why they choose micromanage statements like that?
    private static final String savepointCreate = ";SAVEPOINT android_sqlite_transaction";
    private static final String savepointRelease = ";RELEASE SAVEPOINT android_sqlite_transaction";
    private static final String savepointRollback = ";ROLLBACK TO SAVEPOINT android_sqlite_transaction";

    AndroidSQLiteNestedTransaction(AndroidSQLiteAbstractDatabase parent) {
        super(parent);

        try {
            connection.execSQL(savepointCreate);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    void commitTransaction() {
        connection.execSQL(savepointRelease);
    }

    @Override
    void rollbackTransaction() {
        connection.execSQL(savepointRollback);
    }

}
