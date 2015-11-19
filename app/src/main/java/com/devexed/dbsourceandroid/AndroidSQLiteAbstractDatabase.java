package com.devexed.dbsourceandroid;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;

import com.devexed.dbsource.AbstractCloseable;
import com.devexed.dbsource.Database;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.ExecutionStatement;
import com.devexed.dbsource.InsertStatement;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.QueryStatement;
import com.devexed.dbsource.UpdateStatement;

import java.util.Map;

abstract class AndroidSQLiteAbstractDatabase extends AbstractCloseable implements Database {

    final SQLiteDatabase connection;
    final AndroidSQLiteAccessorFactory accessorFactory;

    private String version = null;
    private AndroidSQLiteTransaction child = null;

    AndroidSQLiteAbstractDatabase(SQLiteDatabase connection, AndroidSQLiteAccessorFactory accessorFactory) {
        this.connection = connection;
        this.accessorFactory = accessorFactory;
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void checkChildClosed() {
        if (child != null) throw new DatabaseException("Child transaction is still open");
    }

    final void onCloseChild() {
        if (child == null) throw new DatabaseException("No child transaction open");
        child = null;
    }

    final void onOpenChild(AndroidSQLiteTransaction child) {
        checkChildClosed();
        this.child = child;
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
    public void close() {
        if (isClosed()) return;

        // Close child hierarchy, allowing easy cleanup on failure.
        if (child != null) child.close();

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
