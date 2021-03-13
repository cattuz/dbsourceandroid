package com.devexed.dalwitandroid;

import org.sqlite.database.SQLException;

import com.devexed.dalwit.Accessor;

/**
 * Accessor to bind values to and retrieve values from Android SQLite interfaces.
 */
public interface AndroidSQLiteAccessor extends Accessor<AndroidSQLiteBindable, android.database.Cursor, SQLException> {
}
