package com.devexed.dbsourceandroid;

import android.database.SQLException;

import com.devexed.dbsource.DatabaseException;

final class AndroidSQLiteRootTransaction extends AndroidSQLiteTransaction {

	AndroidSQLiteRootTransaction(AndroidSQLiteAbstractDatabase parent) {
		super(parent);
	}

    @Override
    void beginTransaction() {
        try {
            connection.beginTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    void commitTransaction() {
        try {
            connection.setTransactionSuccessful();
            connection.endTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    void rollbackTransaction() {
        try {
            connection.endTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
