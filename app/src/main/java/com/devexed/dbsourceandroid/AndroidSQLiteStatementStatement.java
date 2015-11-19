package com.devexed.dbsourceandroid;

import android.database.SQLException;
import android.database.sqlite.SQLiteStatement;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

abstract class AndroidSQLiteStatementStatement extends AndroidSQLiteStatement {

    final SQLiteStatement statement;
    final SQLiteBindable bindable;

    private final HashMap<String, ArrayList<Integer>> parameterIndexes;

    public AndroidSQLiteStatementStatement(AndroidSQLiteAbstractDatabase database, Query query, Map<String, Class<?>> keys) {
        super(database, query);

        parameterIndexes = new HashMap<String, ArrayList<Integer>>();

        try {
            statement = database.connection.compileStatement(query.create(database, parameterIndexes));
            bindable = new SQLiteStatementBindable(statement);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public AndroidSQLiteStatementStatement(AndroidSQLiteAbstractDatabase database, Query query) {
        this(database, query, null);
    }

    @Override
    public void clear() {
        try {
            statement.clearBindings();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public final <T> void bind(String parameter, T value) {
        checkNotClosed();

        try {
            Class<?> type = query.typeOf(parameter);
            if (type == null) throw new DatabaseException("No type is defined for parameter " + parameter);

            AndroidSQLiteAccessor accessor = database.accessorFactory.create(type);
            if (accessor == null) throw new DatabaseException("No accessor is defined for parameter " + parameter);

            ArrayList<Integer> indexes = parameterIndexes.get(parameter);
            if (indexes == null) throw new DatabaseException("No mapping for parameter " + parameter);

            for (int index : indexes) accessor.set(bindable, index + 1, value);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    @SuppressWarnings("TryWithIdenticalCatches")
    public final void close() {
        super.close();

        try {
            statement.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
