package com.devexed.dalwitandroid;

import org.sqlite.database.SQLException;
import com.devexed.dalwit.Accessor;
import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.util.AbstractCloseable;

import java.util.Map;

final class AndroidSQLiteCursor extends AbstractCloseable implements Cursor {

    private final android.database.Cursor cursor;
    private final Map<String, Getter<?>> columns;

    AndroidSQLiteCursor(android.database.Cursor cursor, Map<String, Getter<?>> columns) {
        this.cursor = cursor;
        this.columns = columns;
    }

    @Override
    protected boolean isClosed() {
        return cursor.isClosed() || super.isClosed();
    }

    @Override
    public boolean seek(int rows) {
        checkNotClosed();

        try {
            return cursor.move(rows);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean previous() {
        checkNotClosed();

        try {
            return cursor.moveToPrevious();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean next() {
        checkNotClosed();

        try {
            return cursor.moveToNext();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Getter<T> getter(final String column) {
        return (Getter<T>) columns.get(column.toLowerCase());
    }

    @Override
    public void close() {
        try {
            cursor.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        super.close();
    }

    static final class SQLiteGetter implements Cursor.Getter<Object> {

        private final Accessor<AndroidSQLiteBindable, android.database.Cursor, SQLException> accessor;
        private final android.database.Cursor cursor;
        private final int index;

        SQLiteGetter(Accessor<AndroidSQLiteBindable, android.database.Cursor, SQLException> accessor, android.database.Cursor cursor, int index) {
            this.accessor = accessor;
            this.cursor = cursor;
            this.index = index;
        }

        @Override
        public Object get() {
            try {
                return accessor.get(cursor, index);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

    }

}
