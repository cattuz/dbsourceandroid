package com.devexed.dbsourceandroid;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;

import com.devexed.dbsource.AbstractCloseable;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.ExecutionStatement;
import com.devexed.dbsource.InsertStatement;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.QueryStatement;
import com.devexed.dbsource.TransactionDatabase;
import com.devexed.dbsource.UpdateStatement;

import java.util.Map;

abstract class AndroidSQLiteAbstractDatabase extends AbstractCloseable implements TransactionDatabase {

	final SQLiteDatabase connection;
	final Map<Class<?>, AndroidSQLiteAccessor> accessors;

    private String version = null;
	
	AndroidSQLiteAbstractDatabase(SQLiteDatabase connection, Map<Class<?>, AndroidSQLiteAccessor> accessors) {
		this.connection = connection;
		this.accessors = accessors;
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
	public UpdateStatement prepareUpdate(Query query) {
		checkNotClosed();

        return new AndroidSQLiteUpdateStatement(this, query);
	}

	@Override
	public ExecutionStatement prepareExecution(Query query) {
		checkNotClosed();

        return new AndroidSQLiteExecutionStatement(this, query);
	}

	@Override
	public InsertStatement prepareInsert(Query query, Map<String, Class<?>> keys) {
		checkNotClosed();

        return new AndroidSQLiteInsertStatement(this, query, keys);
	}
	
	@Override
	public String toString() {
		String url;
		
		try {
			url = connection.getPath();
		} catch (SQLException e) {
			url = ":unavailable:";
		}
		
		return "[" + AndroidSQLiteAbstractDatabase.class.getSimpleName() + "; " +
                "type=" + getType() + "; " +
                "version=" + getVersion() + "; " +
                "url=" + url + "]";
	}
	
}
