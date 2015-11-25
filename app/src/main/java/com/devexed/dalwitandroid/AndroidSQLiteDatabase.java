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
        super("database", connection, accessorFactory);
    }

    @Override
    public Transaction transact() {
        checkNotClosed();
        return openChildTransaction(new AndroidSQLiteRootTransaction(this));
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        super.close();
    }

}
