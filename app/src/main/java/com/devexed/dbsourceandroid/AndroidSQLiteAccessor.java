package com.devexed.dbsourceandroid;

import android.database.Cursor;
import android.database.SQLException;

import com.devexed.dbsource.Accessor;

/**
 * Accessor to bind values to and retrieve values from Android SQLite interfaces.
 */
public interface AndroidSQLiteAccessor extends Accessor<SQLiteBindable, Cursor, SQLException> {
}
