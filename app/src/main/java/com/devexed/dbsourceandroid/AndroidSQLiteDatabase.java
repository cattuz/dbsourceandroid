package com.devexed.dbsourceandroid;

import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;

import com.devexed.dbsource.Database;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

public final class AndroidSQLiteDatabase extends AndroidSQLiteAbstractDatabase {

    private AndroidSQLiteDatabase(SQLiteDatabase connection, AndroidSQLiteAccessorFactory accessorFactory) {
        super(connection, accessorFactory);
    }

    public static Database open(SQLiteDatabase connection, AndroidSQLiteAccessorFactory accessorFactory) {
        return new AndroidSQLiteDatabase(connection, accessorFactory);
    }

    public static Database open(SQLiteDatabase connection) {
        return new AndroidSQLiteDatabase(connection, new DefaultAndroidSQLiteAccessorFactory());
    }

    /*
        private static void runPragma(SQLiteDatabase connection, String pragma) {
            // Runs a pragma on the database. Apparently needs to be a query whose cursor is actually used for the pragma to
            // take effect.
            Cursor cursor = connection.rawQuery(pragma, new String[0]);
            cursor.moveToFirst();
            cursor.close();
        }

        private static AndroidSQLiteDatabase open(SQLiteDatabase connection, AndroidSQLiteAccessorFactory accessorFactory) {
            return new AndroidSQLiteDatabase(connection, accessorFactory);
            try {
                int flags = writable ? SQLiteDatabase.CREATE_IF_NECESSARY : SQLiteDatabase.OPEN_READONLY;
                SQLiteDatabase connection = SQLiteDatabase.openDatabase(url, null, flags);
                runPragma(connection, "PRAGMA foreign_keys=on");
                runPragma(connection, "PRAGMA journal_mode=delete");

                return new AndroidSQLiteDatabase(connection, accessorFactory);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    */
    @Override
    public Transaction transact() {
        checkNotClosed();

        AndroidSQLiteTransaction transaction = new AndroidSQLiteRootTransaction(this);
        onOpenChild(transaction);
        return transaction;
    }

    @Override
    public void close() {
        if (isClosed()) return;

        super.close();

        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
