package com.devexed.dalwitandroid;

import android.database.Cursor;
import android.database.SQLException;

import com.devexed.dalwit.Accessor;

/**
 * Accessor to bind values to and retrieve values from Android SQLite interfaces.
 */
public interface AndroidSQLiteAccessor extends Accessor<SQLiteBindable, android.database.Cursor, SQLException> {
}
