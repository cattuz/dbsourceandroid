package com.devexed.dbsourceandroid;

import android.database.SQLException;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteCursorDriver;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQuery;
import android.util.SparseArray;

import com.devexed.dbsource.DatabaseCursor;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.QueryStatement;

import java.util.HashMap;

final class AndroidSQLiteQueryStatement extends AndroidSQLiteStatement implements QueryStatement {

    private static final class Binding {
        final Object value;
        final AndroidSQLiteAccessor accessor;

        private Binding(Object value, AndroidSQLiteAccessor accessor) {
            this.value = value;
            this.accessor = accessor;
        }
    }

    private final String queryString;
    private final SparseArray<Binding> parameterValues;
    private final HashMap<String, int[]> parameterIndexes;
    private final SQLiteDatabase.CursorFactory cursorFactory;

	public AndroidSQLiteQueryStatement(AndroidSQLiteAbstractDatabase database, Query query) {
		super(database, query);
        parameterIndexes = new HashMap<String, int[]>();
        parameterValues = new SparseArray<Binding>();
        queryString = query.create(database, parameterIndexes);
        cursorFactory = new SQLiteDatabase.CursorFactory() {

            @Override
            public android.database.Cursor newCursor(SQLiteDatabase db, SQLiteCursorDriver masterQuery,
                                                     String editTable, SQLiteQuery query) {
                for (int i = 0, l = parameterValues.size(); i < l; i++) {
                    int index = parameterValues.keyAt(i);
                    Binding binding = parameterValues.get(index);
                    binding.accessor.setQuery(query, index, binding.value);
                }

                return new SQLiteCursor(masterQuery, null, query);
            }

        };
	}

    @Override
    public void clear() {
        parameterValues.clear();
    }

    @Override
    public <T> void bind(String parameter, T value) {
        Class<?> type = query.typeOf(parameter);
        AndroidSQLiteAccessor accessor = database.accessors.get(type);

        if (accessor == null) throw new DatabaseException("No accessor is defined for type " + type);

        int[] indexes = parameterIndexes.get(parameter);

        if (indexes == null) throw new DatabaseException("No mapping for parameter " + parameter);

        Binding binding = new Binding(value, accessor);

        for (int index: indexes) parameterValues.put(index + 1, binding);
    }

    @Override
	public DatabaseCursor query() {
		checkNotClosed();

		try {
			return new AndroidSQLiteCursor(new AndroidSQLiteCursor.AccessorFunction() {
                @Override
                public AndroidSQLiteAccessor accessorOf(String column) {
                    return database.accessors.get(query.typeOf(column));
                }
            }, database.connection.rawQueryWithFactory(cursorFactory, queryString, new String[0], null));
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

    @Override
    public void close() {
        super.close();
    }

}
