package com.devexed.dbsourceandroid;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQuery;
import android.database.sqlite.SQLiteStatement;

import com.devexed.dbsource.Database;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;
import com.devexed.dbsource.TransactionDatabase;

import java.util.HashMap;
import java.util.Map;

public final class AndroidSQLiteDatabase extends AndroidSQLiteAbstractDatabase {

    /**
     * Definitions for core java accessors that have a corresponding SQLite setter and getter.
     */
    public static final Map<Class<?>, AndroidSQLiteAccessor> accessors = new HashMap<Class<?>, AndroidSQLiteAccessor>() {{
        put(Boolean.TYPE, new AndroidSQLiteAccessor() {

            @Override
            public void setStatement(SQLiteStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type boolean can not be null.");
                statement.bindLong(index, (boolean) value ? 1 : 0);
            }

            @Override
            public void setQuery(SQLiteQuery query, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type boolean can not be null.");
                query.bindLong(index, (boolean) value ? 1 : 0);
            }

            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index))
                    throw new NullPointerException("Illegal null value for type boolean in result set.");
                return cursor.getLong(index) == 1;
            }

        });
    }};

    public static Database openReadable(String url, Map<Class<?>, AndroidSQLiteAccessor> accessors) {
        return open(url, accessors, false);
    }

    public static TransactionDatabase openWritable(String url, Map<Class<?>, AndroidSQLiteAccessor> accessors) {
        return open(url, accessors, true);
    }

    private static void runPragma(SQLiteDatabase connection, String pragma) {
        // Runs a pragma on the database. Apparently needs to be a query whose cursor is actually used for the pragma to
        // take effect.
        Cursor cursor = connection.rawQuery(pragma, new String[0]);
        cursor.moveToFirst();
        cursor.close();
    }

	private static AndroidSQLiteDatabase open(String url, Map<Class<?>, AndroidSQLiteAccessor> accessors,
                                              boolean writable) {
		try {
            int flags = writable ? SQLiteDatabase.OPEN_READONLY : SQLiteDatabase.CREATE_IF_NECESSARY;
            SQLiteDatabase connection = SQLiteDatabase.openDatabase(url, null, flags);
            runPragma(connection, "PRAGMA foreign_keys=on");
            runPragma(connection, "PRAGMA journal_mode=delete");

			return new AndroidSQLiteDatabase(connection, accessors);
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

    private AndroidSQLiteDatabase(SQLiteDatabase connection, Map<Class<?>, AndroidSQLiteAccessor> accessors) {
		super(connection, accessors);
	}

	@Override
	public Transaction transact() {
		checkNotClosed();

		return new AndroidSQLiteTransaction(this);
	}

	@Override
	public void close() {
		checkNotClosed();

		try {
			connection.close();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

}
