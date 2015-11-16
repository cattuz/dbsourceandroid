package com.devexed.dbsourceandroid;

import android.database.SQLException;

import com.devexed.dbsource.AbstractCloseable;
import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.DatabaseException;

final class AndroidSQLiteCursor extends AbstractCloseable implements Cursor {

	public interface AccessorFunction {

		AndroidSQLiteAccessor accessorOf(String column);

	}
	
	private static int columnIndexOf(int index) {
		return index + 1;
	}

    private final AccessorFunction typeOfFunction;
	private final android.database.Cursor cursor;
	
	AndroidSQLiteCursor(AccessorFunction typeOfFunction, android.database.Cursor cursor) {
		this.typeOfFunction = typeOfFunction;
		this.cursor = cursor;
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
            int index = cursor.getColumnIndex(column);

			if (index < 0) throw new DatabaseException("No such column " + column);

			AndroidSQLiteAccessor accessor = typeOfFunction.accessorOf(column);

			if (accessor == null) throw new DatabaseException("No accessor is defined for column " + column);

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

}
