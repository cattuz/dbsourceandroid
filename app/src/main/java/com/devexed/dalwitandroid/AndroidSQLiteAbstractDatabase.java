package com.devexed.dalwitandroid;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import com.devexed.dalwit.*;
import com.devexed.dalwit.util.AbstractCloseable;
import com.devexed.dalwit.util.AbstractCloseableCloser;

import java.util.Map;

abstract class AndroidSQLiteAbstractDatabase extends AbstractCloseable implements Database {

    final SQLiteDatabase connection;
    final AccessorFactory<SQLiteBindable, Integer, Cursor, Integer, SQLException> accessorFactory;
    final AbstractCloseableCloser<Statement, AndroidSQLiteStatement> statementManager;

    private final Class<?> managerType;

    private String version = null;
    private AndroidSQLiteTransaction child = null;

    AndroidSQLiteAbstractDatabase(Class<?> managerType, AbstractCloseableCloser<Statement, AndroidSQLiteStatement> statementManager, SQLiteDatabase connection, AccessorFactory<SQLiteBindable, Integer, Cursor, Integer, SQLException> accessorFactory) {
        this.managerType = managerType;
        this.statementManager = statementManager;
        this.connection = connection;
        this.accessorFactory = accessorFactory;
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void checkActive() {
        if (child != null) throw new DatabaseException("Child transaction is still open");
        checkNotClosed();
    }

    final AndroidSQLiteTransaction openTransaction(AndroidSQLiteTransaction child) {
        checkActive();
        this.child = child;
        return child;
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void checkTransaction(Transaction transaction) {
        if (transaction == null) throw new NullPointerException("Child transaction is null");

        if (transaction != child)
            throw new DatabaseException("Child transaction was not started by this " + managerType);
    }

    @Override
    public void commit(Transaction transaction) {
        checkTransaction(transaction);
        child.checkActive();

        try {
            child.commitTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        child.close();
        child = null;
    }

    @Override
    public void rollback(Transaction transaction) {
        checkTransaction(transaction);
        child.checkActive();

        try {
            child.rollbackTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        child.close();
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

        return statementManager.open(new AndroidSQLiteQueryStatement(this, query));
    }

    @Override
    public UpdateStatement createUpdate(Query query) {
        checkNotClosed();

        return statementManager.open(new AndroidSQLiteUpdateStatement(this, query));
    }

    @Override
    public ExecutionStatement createExecution(Query query) {
        checkNotClosed();

        return statementManager.open(new AndroidSQLiteExecutionStatement(this, query));
    }

    @Override
    public InsertStatement createInsert(Query query, Map<String, Class<?>> keys) {
        checkNotClosed();

        return statementManager.open(new AndroidSQLiteInsertStatement(this, query, keys));
    }

    @Override
    public void close(Statement statement) {
        statementManager.close(statement);
    }

    @Override
    public void close() {
        statementManager.close();
        super.close();
    }

    @Override
    public String toString() {
        return "[" + AndroidSQLiteAbstractDatabase.class.getSimpleName() + "; " +
                "type=" + getType() + "; " +
                "version=" + getVersion() + "; " +
                "url=" + connection.getPath() + "]";
    }

}
