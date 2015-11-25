package com.devexed.dalwitandroid;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import com.devexed.dalwit.*;
import com.devexed.dalwit.util.AbstractCloseable;

import java.util.Map;

abstract class AndroidSQLiteAbstractDatabase extends AbstractCloseable implements Database {

    final SQLiteDatabase connection;
    final AccessorFactory<SQLiteBindable, Integer, Cursor, Integer, SQLException> accessorFactory;

    private final String managerType;
    private String version = null;
    private AndroidSQLiteTransaction child = null;

    AndroidSQLiteAbstractDatabase(String managerType, SQLiteDatabase connection, AccessorFactory<SQLiteBindable, Integer, Cursor, Integer, SQLException> accessorFactory) {
        this.managerType = managerType;
        this.connection = connection;
        this.accessorFactory = accessorFactory;
    }

    @Override
    protected final boolean isClosed() {
        try {
            return super.isClosed() || !connection.isOpen();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close() {
        if (child != null) closeChildTransaction(child);
        super.close();
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void checkActive() {
        if (child != null) throw new DatabaseException("Child transaction is still open");
        checkNotClosed();
    }

    final AndroidSQLiteTransaction openChildTransaction(AndroidSQLiteTransaction child) {
        checkActive();
        this.child = child;
        return child;
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void checkChildTransaction(Transaction transaction) {
        if (transaction == null) throw new NullPointerException("Child transaction is null");

        if (transaction != child)
            throw new DatabaseException("Child transaction was not started by this " + managerType);
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void closeChildTransaction(Transaction transaction) {
        checkChildTransaction(transaction);
        child = null;
    }

    @Override
    public String getType() {
        checkNotClosed();
        return "SQLite";
    }

    @Override
    public String getVersion() {
        checkNotClosed();

        // Query the database for the version and cache it.
        if (version == null) {
            try {
                Cursor cursor = null;

                try {
                    cursor = connection.rawQuery("SELECT sqlite_version()", null);
                    if (cursor.moveToNext()) version = cursor.getString(0);
                } finally {
                    if (cursor != null) cursor.close();
                }
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        return version;
    }

    @Override
    public QueryStatement createQuery(Query query) {
        checkNotClosed();
        return new AndroidSQLiteQueryStatement(this, query);
    }

    @Override
    public UpdateStatement createUpdate(Query query) {
        checkNotClosed();
        return new AndroidSQLiteUpdateStatement(this, query);
    }

    @Override
    public ExecutionStatement createExecution(Query query) {
        checkNotClosed();
        return new AndroidSQLiteExecutionStatement(this, query);
    }

    @Override
    public InsertStatement createInsert(Query query, Map<String, Class<?>> keys) {
        checkNotClosed();
        return new AndroidSQLiteInsertStatement(this, query, keys);
    }

    @Override
    public String toString() {
        return "[" + AndroidSQLiteAbstractDatabase.class.getSimpleName() + "; " +
                "type=" + getType() + "; " +
                "version=" + getVersion() + "; " +
                "url=" + connection.getPath() + "]";
    }

}
