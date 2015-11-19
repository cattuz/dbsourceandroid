package com.devexed.dbsourceandroid;

import android.database.SQLException;

import com.devexed.dbsource.AbstractCloseable;
import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.DatabaseException;

final class AndroidSQLiteCursor extends AbstractCloseable implements Cursor {

    private final AndroidSQLiteAccessorFactory accessorFactory;
    private final TypeFunction typeOfFunction;
    private final android.database.Cursor cursor;

    AndroidSQLiteCursor(AndroidSQLiteAccessorFactory accessorFactory, TypeFunction typeOfFunction, android.database.Cursor cursor) {
        this.accessorFactory = accessorFactory;
        this.typeOfFunction = typeOfFunction;
        this.cursor = cursor;
    }

    private static int columnIndexOf(int index) {
        return index + 1;
    }

    @Override
    protected boolean isClosed() {
        return cursor.isClosed() || super.isClosed();
    }

    @Override
    public boolean seek(int rows) {
        checkNotClosed();

        try {
            if (cursor.move(rows)) return true;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        close();

        return false;
    }

    @Override
    public boolean previous() {
        return seek(-1);
    }

    @Override
    public boolean next() {
        return seek(1);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String column) {
        checkNotClosed();

        try {
            Class<?> type = typeOfFunction.typeOf(column);
            if (type == null) throw new DatabaseException("No such column " + column);

            AndroidSQLiteAccessor accessor = accessorFactory.create(type);
            int index = cursor.getColumnIndexOrThrow(column);

            return (T) accessor.get(cursor, index);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close() {
        if (isClosed()) return;

        try {
            cursor.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        super.close();
    }

    public interface TypeFunction {

        Class<?> typeOf(String column);

    }

}
