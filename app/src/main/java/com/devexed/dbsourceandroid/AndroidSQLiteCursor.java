package com.devexed.dbsourceandroid;

import android.database.Cursor;
import android.database.SQLException;

import com.devexed.dbsource.AbstractCloseable;
import com.devexed.dbsource.DatabaseCursor;
import com.devexed.dbsource.DatabaseException;

final class AndroidSQLiteCursor extends AbstractCloseable implements DatabaseCursor {

	public interface AccessorFunction {

		AndroidSQLiteAccessor accessorOf(String column);

	}
	
	private static int columnIndexOf(int index) {
		return index + 1;
	}

    private final AccessorFunction typeOfFunction;
	private final Cursor cursor;
	
	AndroidSQLiteCursor(AccessorFunction typeOfFunction, Cursor cursor) {
		this.typeOfFunction = typeOfFunction;
		this.cursor = cursor;
	}

    @Override
    protected void checkNotClosed() {
        super.checkNotClosed();

        if (cursor.isClosed()) throw new DatabaseException("Cursor is closed.");
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
	public <T> T get(String column) {
		checkNotClosed();

		try {
            int index = cursor.getColumnIndex(column);

			if (index < 0) throw new DatabaseException("Column " + column + " not found.");

			AndroidSQLiteAccessor accessor = typeOfFunction.accessorOf(column);

			if (accessor == null) throw new DatabaseException("No accessor is defined for column " + column + ".");

			return (T) accessor.get(cursor, index);
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

    @Override
    public void close() {
        super.close();

        try {
            cursor.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
