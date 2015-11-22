package com.devexed.dbsourceandroid;

import android.database.SQLException;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteCursorDriver;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQuery;

import com.devexed.dbsource.Accessor;
import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.QueryStatement;
import com.devexed.dbsource.ReadonlyDatabase;
import com.devexed.dbsource.util.CloseableManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

final class AndroidSQLiteQueryStatement extends AndroidSQLiteStatement implements QueryStatement {

    private final String queryString;
    private final HashMap<String, Binding> parameterBindings;
    private final HashMap<String, ArrayList<Integer>> parameterIndexes;
    private final SQLiteDatabase.CursorFactory cursorFactory;
    private final CloseableManager<AndroidSQLiteCursor> cursorManager =
            new CloseableManager<AndroidSQLiteCursor>(QueryStatement.class, Cursor.class);

    public AndroidSQLiteQueryStatement(AndroidSQLiteAbstractDatabase database, Query query) {
        super(database, query);
        parameterIndexes = new HashMap<String, ArrayList<Integer>>();
        parameterBindings = new HashMap<String, Binding>();
        queryString = query.create(database, parameterIndexes);
        cursorFactory = new SQLiteDatabase.CursorFactory() {

            @Override
            public android.database.Cursor newCursor(SQLiteDatabase db, SQLiteCursorDriver masterQuery,
                                                     String editTable, SQLiteQuery query) {
                // Check all parameters have been bound.
                HashSet<String> unboundParameters = new HashSet<String>(parameterIndexes.keySet());
                unboundParameters.removeAll(parameterBindings.keySet());

                if (!unboundParameters.isEmpty())
                    throw new DatabaseException("Unbound parameters " + unboundParameters.toString());

                // Bind parameters to query.
                SQLiteBindable bindable = new SQLiteQueryBindable(query);

                for (Binding binding : parameterBindings.values()) {
                    for (int index : binding.indexes) binding.accessor.set(bindable, index + 1, binding.value);
                }

                return new SQLiteCursor(masterQuery, null, query);
            }

        };
    }

    @Override
    public void clear() {
        parameterBindings.clear();
    }

    @Override
    public <T> void bind(String parameter, T value) {
        Class<?> type = query.typeOf(parameter);
        if (type == null) throw new DatabaseException("No type is defined for parameter " + parameter);

        Accessor<SQLiteBindable, android.database.Cursor, SQLException> accessor = database.accessorFactory.create(type);
        if (accessor == null) throw new DatabaseException("No accessor is defined for parameter " + parameter);

        ArrayList<Integer> indexes = parameterIndexes.get(parameter);
        if (indexes == null) throw new DatabaseException("Undefined parameter " + parameter);

        Binding binding = new Binding(value, accessor, indexes);
        parameterBindings.put(parameter, binding);
    }

    @Override
    public Cursor query(ReadonlyDatabase database) {
        checkNotClosed();
        checkActiveDatabase(database);

        try {
            return cursorManager.open(new AndroidSQLiteCursor(
                    this.database.connection.rawQueryWithFactory(cursorFactory, queryString, new String[0], null),
                    this.database.accessorFactory,
                    new AndroidSQLiteCursor.TypeFunction() {
                @Override
                public Class<?> typeOf(String column) {
                    return query.typeOf(column);
                }
                    }));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close(Cursor cursor) {
        cursorManager.close(cursor);
    }

    @Override
    public void close() {
        cursorManager.close();
        super.close();
    }

    private static final class Binding {
        final Object value;
        final Accessor<SQLiteBindable, android.database.Cursor, SQLException> accessor;
        final ArrayList<Integer> indexes;

        Binding(Object value, Accessor<SQLiteBindable, android.database.Cursor, SQLException> accessor,
                ArrayList<Integer> indexes) {
            this.value = value;
            this.accessor = accessor;
            this.indexes = indexes;
        }
    }

}
