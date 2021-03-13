package com.devexed.dalwitandroid;

import android.annotation.SuppressLint;
import org.sqlite.database.SQLException;
import org.sqlite.database.sqlite.SQLiteCursor;
import org.sqlite.database.sqlite.SQLiteCursorDriver;
import org.sqlite.database.sqlite.SQLiteDatabase;
import org.sqlite.database.sqlite.SQLiteQuery;
import org.sqlite.database.sqlite.SQLiteStatement;
import android.util.SparseArray;

import com.devexed.dalwit.Accessor;
import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.ListBinder;
import com.devexed.dalwit.Query;
import com.devexed.dalwit.Statement;
import com.devexed.dalwit.util.AbstractCloseable;
import com.devexed.dalwit.util.Cursors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AndroidSQLiteStatement extends AbstractCloseable implements Statement {

    private final AndroidSQLiteAbstractDatabase database;
    private final Query query;
    private final String key;

    // Cursor factory if used as a query statement
    private SQLiteDatabase.CursorFactory cursorFactory = null;

    // Compiled statement if used as an update statement
    private SQLiteStatement statement = null;
    private AndroidSQLiteBindable bindable = new StoredSQLiteBindable();

    @SuppressLint("UseSparseArrays")
    public AndroidSQLiteStatement(AndroidSQLiteAbstractDatabase database, final Query query) {
        this.database = database;
        this.query = query;

        // Grab key from query, only a single `long` key is supported
        Iterator<Map.Entry<String, Class<?>>> keyIterator = query.keys().entrySet().iterator();

        if (keyIterator.hasNext()) {
            Map.Entry<String, Class<?>> keyEntry = keyIterator.next();

            if (!keyEntry.getValue().equals(Long.TYPE)) {
                throw new DatabaseException("Only generated keys of type `long` are supported");
            }

            this.key = keyEntry.getKey();

            if (keyIterator.hasNext()) {
                throw new DatabaseException("Only a single key is supported");
            }
        } else {
            this.key = "rowid";
        }
    }

    private void prepareCompiledStatement() {
        if (statement != null) {
            return;
        }

        if (cursorFactory != null || !(bindable instanceof StoredSQLiteBindable)) {
            throw new DatabaseException("Cannot use statement to query after it has been used to update");
        }

        try {
            statement = database.connection.compileStatement(query.sql());

            // Bind all stored parameter values
            StoredSQLiteBindable storedBindable = (StoredSQLiteBindable) bindable;

            for (int i = 0, l = storedBindable.nulls.size(); i < l; i++) {
                int index = storedBindable.nulls.keyAt(i);
                statement.bindNull(index + 1);
            }

            for (int i = 0, l = storedBindable.longs.size(); i < l; i++) {
                int index = storedBindable.longs.keyAt(i);
                statement.bindLong(index + 1, storedBindable.longs.valueAt(i));
            }

            for (int i = 0, l = storedBindable.doubles.size(); i < l; i++) {
                int index = storedBindable.doubles.keyAt(i);
                statement.bindDouble(index + 1, storedBindable.doubles.valueAt(i));
            }

            for (int i = 0, l = storedBindable.strings.size(); i < l; i++) {
                int index = storedBindable.strings.keyAt(i);
                statement.bindString(index + 1, storedBindable.strings.valueAt(i));
            }

            for (int i = 0, l = storedBindable.blobs.size(); i < l; i++) {
                int index = storedBindable.blobs.keyAt(i);
                statement.bindBlob(index + 1, storedBindable.blobs.valueAt(i));
            }

            // Redirect new bindings directly to the statement
            bindable = new SQLiteStatementBindable(statement);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private void prepareCursorFactory() {
        if (cursorFactory != null) {
            return;
        }

        if (statement != null || !(bindable instanceof StoredSQLiteBindable)) {
            throw new DatabaseException("Cannot use statement to update after it has been used to query");
        }

        try {
            final StoredSQLiteBindable storedBindable = (StoredSQLiteBindable) bindable;
            cursorFactory = new SQLiteDatabase.CursorFactory() {

                @Override
                public android.database.Cursor newCursor(SQLiteDatabase db, SQLiteCursorDriver masterQuery,
                                                         String editTable, SQLiteQuery sqliteQuery) {
                    // Bind all stored parameter values
                    for (int i = 0, l = storedBindable.nulls.size(); i < l; i++) {
                        int index = storedBindable.nulls.keyAt(i);
                        sqliteQuery.bindNull(index + 1);
                    }

                    for (int i = 0, l = storedBindable.longs.size(); i < l; i++) {
                        int index = storedBindable.longs.keyAt(i);
                        sqliteQuery.bindLong(index + 1, storedBindable.longs.valueAt(i));
                    }

                    for (int i = 0, l = storedBindable.doubles.size(); i < l; i++) {
                        int index = storedBindable.doubles.keyAt(i);
                        sqliteQuery.bindDouble(index + 1, storedBindable.doubles.valueAt(i));
                    }

                    for (int i = 0, l = storedBindable.strings.size(); i < l; i++) {
                        int index = storedBindable.strings.keyAt(i);
                        sqliteQuery.bindString(index + 1, storedBindable.strings.valueAt(i));
                    }

                    for (int i = 0, l = storedBindable.blobs.size(); i < l; i++) {
                        int index = storedBindable.blobs.keyAt(i);
                        sqliteQuery.bindBlob(index + 1, storedBindable.blobs.valueAt(i));
                    }

                    return new SQLiteCursor(masterQuery, editTable, sqliteQuery);
                }

            };
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public final <T> Binder<T> binder(String parameter) {
        checkNotClosed();
        Integer listSize = query.parameterListSizes().get(parameter.toLowerCase());

        if (listSize == null) {
            String parameterName = parameter.toLowerCase();
            Class<?> parameterType = query.parameters().get(parameterName);

            if (parameterType == null) {
                throw new DatabaseException("No type is defined for parameter " + parameter);
            }

            final Accessor<AndroidSQLiteBindable, android.database.Cursor, SQLException> accessor = database.accessorFactory.create(parameterType);

            if (accessor == null) {
                throw new DatabaseException("No accessor is defined for parameter " + parameter);
            }

            final int[] indexes = query.parameterIndices().get(parameter);

            if (indexes == null) {
                throw new DatabaseException("No mapping for parameter " + parameter);
            }

            return new Binder<T>() {
                @Override
                public void bind(T value) {
                    try {
                        for (int index : indexes) accessor.set(bindable, index + 1, value);
                    } catch (SQLException e) {
                        throw new DatabaseException(e);
                    }
                }
            };
        } else {
            ArrayList<Binder<Object>> binders = new ArrayList<>(listSize);

            for (int i = 0; i < listSize; i++) {
                binders.add(binder(Query.parameterListIndexer(parameter, i)));
            }

            return new ListBinder<>(binders);
        }
    }

    @Override
    public void execute() {
        checkNotClosed();
        database.checkActive();

        try {
            prepareCompiledStatement();
            statement.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public long update() {
        checkNotClosed();
        database.checkActive();

        try {
            prepareCompiledStatement();
            return statement.executeUpdateDelete();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public com.devexed.dalwit.Cursor insert() {
        checkNotClosed();
        database.checkActive();

        try {
            prepareCompiledStatement();
            final long generatedKey = statement.executeInsert();

            if (generatedKey < 0) return Cursors.empty();

            return Cursors.singleton(key, generatedKey);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public com.devexed.dalwit.Cursor query() {
        checkNotClosed();
        database.checkActive();

        try {
            prepareCursorFactory();
            android.database.Cursor cursor = database.connection.rawQueryWithFactory(cursorFactory, query.sql(), new String[0], null);
            Map<String, Cursor.Getter<?>> columns = new HashMap<>();
            String[] columnNames = cursor.getColumnNames();

            for (int i = 0; i < columnNames.length; i++) {
                String rawColumnName = columnNames[i];

                if (rawColumnName.startsWith("\"") && rawColumnName.endsWith("\"")) {
                    rawColumnName = rawColumnName
                            .substring(1, rawColumnName.length() - 1)
                            .replace("\"\"", "\"");
                }

                String mappedColumnName = database.columnNameMapper.apply(rawColumnName).toLowerCase();
                String columnName = null;
                Class<?> columnType = null;

                for (String c : new String[]{ rawColumnName.toLowerCase(), mappedColumnName }) {
                    columnName = c;
                    columnType = query.columns().get(c);

                    if (columnType != null)
                        break;
                }

                if (columnType != null) {
                    Accessor<AndroidSQLiteBindable, android.database.Cursor, SQLException> accessor = database.accessorFactory.create(columnType);

                    if (accessor == null) {
                        throw new DatabaseException("No accessor is defined for type " + columnType + " (column " + rawColumnName + ")");
                    }

                    AndroidSQLiteCursor.SQLiteGetter getter = new AndroidSQLiteCursor.SQLiteGetter(accessor, cursor, i);
                    columns.put(columnName, getter);

                    if (!columnName.equals(mappedColumnName)) {
                        columns.put(mappedColumnName, getter);
                    }
                }
            }

            return new AndroidSQLiteCursor(cursor, columns);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close() {
        super.close();

        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private static final class StoredSQLiteBindable implements AndroidSQLiteBindable {
        private final SparseArray<Object> nulls = new SparseArray<>();
        private final SparseArray<Long> longs = new SparseArray<>();
        private final SparseArray<String> strings = new SparseArray<>();
        private final SparseArray<Double> doubles = new SparseArray<>();
        private final SparseArray<byte[]> blobs = new SparseArray<>();

        @Override
        public void bindNull(int index) {
            nulls.put(index, null);
        }

        @Override
        public void bindLong(int index, long value) {
            longs.put(index, value);
        }

        @Override
        public void bindString(int index, String value) {
            strings.put(index, value);
        }

        @Override
        public void bindDouble(int index, double value) {
            doubles.put(index, value);
        }

        @Override
        public void bindBlob(int index, byte[] bytes) {
            blobs.put(index, bytes);
        }
    }

}
