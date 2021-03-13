package com.devexed.dalwitandroid;

import org.sqlite.database.SQLException;
import com.devexed.dalwit.DatabaseException;

final class AndroidSQLiteRootTransaction extends AndroidSQLiteTransaction {

    AndroidSQLiteRootTransaction(AndroidSQLiteAbstractDatabase parent) {
        super(parent);

        try {
            connection.beginTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    void commitTransaction() {
        connection.setTransactionSuccessful();
        connection.endTransaction();
    }

    @Override
    void rollbackTransaction() {
        connection.endTransaction();
    }

}
