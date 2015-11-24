package com.devexed.dalwitandroid;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import com.devexed.dalwit.*;
import com.devexed.dalwit.util.CloseableManager;

final class AndroidSQLiteDatabase extends AndroidSQLiteAbstractDatabase {

    AndroidSQLiteDatabase(SQLiteDatabase connection, AccessorFactory<SQLiteBindable, Integer, Cursor, Integer,
            SQLException> accessorFactory) {
        super(Database.class, new CloseableManager<AndroidSQLiteStatement>(Database.class, Statement.class), connection,
                accessorFactory);
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
