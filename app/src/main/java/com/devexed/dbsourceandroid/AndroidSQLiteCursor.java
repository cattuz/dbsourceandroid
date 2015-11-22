package com.devexed.dbsourceandroid;

import android.database.SQLException;

import com.devexed.dbsource.Accessor;
import com.devexed.dbsource.AccessorFactory;
import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.util.AbstractCloseable;

final class AndroidSQLiteCursor extends AbstractCloseable implements Cursor {

    private final AccessorFactory<SQLiteBindable, android.database.Cursor, SQLException> accessorFactory;
    private final TypeFunction typeOfFunction;
    private final android.database.Cursor cursor;

    AndroidSQLiteCursor(android.database.Cursor cursor, AccessorFactory<SQLiteBindable, android.database.Cursor, SQLException> accessorFactory,
                        TypeFunction typeOfFunction) {
        this.cursor = cursor;
        this.accessorFactory = accessorFactory;
        this.typeOfFunction = typeOfFunction;
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

            Accessor<SQLiteBindable, android.database.Cursor, SQLException> accessor = accessorFactory.create(type);
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
