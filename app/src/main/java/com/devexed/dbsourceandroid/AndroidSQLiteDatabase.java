package com.devexed.dbsourceandroid;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;

import com.devexed.dbsource.AccessorFactory;
import com.devexed.dbsource.Database;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

final class AndroidSQLiteDatabase extends AndroidSQLiteAbstractDatabase {

    AndroidSQLiteDatabase(SQLiteDatabase connection, AccessorFactory<SQLiteBindable, Cursor, SQLException> accessorFactory) {
        super(Database.class, connection, accessorFactory);
    }

    @Override
    public Transaction transact() {
        checkNotClosed();
        return openTransaction(new AndroidSQLiteRootTransaction(this));
    }

    @Override
    public void close() {
        super.close();

        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
