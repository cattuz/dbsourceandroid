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

    private String version = null;
    private AndroidSQLiteTransaction child = null;

    AndroidSQLiteAbstractDatabase(SQLiteDatabase connection, AccessorFactory<SQLiteBindable, Integer, Cursor, Integer, SQLException> accessorFactory) {
        this.connection = connection;
        this.accessorFactory = accessorFactory;
    }

    public SQLiteDatabase getNativeSQLiteDatabase() {
        return connection;
    }

    @Override
    protected final boolean isClosed() {
        try {
            return super.isClosed() || !connection.isOpen();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    abstract void closeResource();

    @Override
    public final void close() {
        if (child != null) {
            child.close();
            child = null;
        }

        closeResource();
        super.close();
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    void checkActive() {
        checkNotClosed();
        if (child != null) throw new DatabaseException("Transaction has child transaction open");
    }

    final AndroidSQLiteTransaction openChildTransaction(AndroidSQLiteTransaction child) {
        checkActive();
        this.child = child;
        return child;
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void checkIsChildTransaction(Transaction transaction) {
        if (transaction != child) throw new DatabaseException("Child transaction not open");
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void closeChildTransaction(Transaction transaction) {
        checkIsChildTransaction(transaction);
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
        checkActive();
        return new AndroidSQLiteQueryStatement(this, query);
    }

    @Override
    public UpdateStatement createUpdate(Query query) {
        checkActive();
        return new AndroidSQLiteUpdateStatement(this, query);
    }

    @Override
    public ExecutionStatement createExecution(Query query) {
        checkActive();
        return new AndroidSQLiteExecutionStatement(this, query);
    }

    @Override
    public InsertStatement createInsert(Query query, Map<String, Class<?>> keys) {
        checkActive();
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
