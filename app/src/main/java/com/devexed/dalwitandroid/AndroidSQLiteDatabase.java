package com.devexed.dalwitandroid;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import com.devexed.dalwit.AccessorFactory;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Transaction;

final class AndroidSQLiteDatabase extends AndroidSQLiteAbstractDatabase {

    AndroidSQLiteDatabase(SQLiteDatabase connection,
                          AccessorFactory<SQLiteBindable, Integer, Cursor, Integer, SQLException> accessorFactory) {
        super(connection, accessorFactory);
    }

    @Override
    public Transaction transact() {
        checkActive();
        return openChildTransaction(new AndroidSQLiteRootTransaction(this));
    }

    @Override
    void closeResource() {
        // Sanity check. Some Android versions will silently keep the SQLite database locked if a transaction is open
        // when closing the connection.
        if (connection.inTransaction()) throw new DatabaseException("Cannot close while child transaction is open");

        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
