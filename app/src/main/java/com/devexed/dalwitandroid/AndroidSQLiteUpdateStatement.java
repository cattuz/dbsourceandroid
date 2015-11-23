package com.devexed.dalwitandroid;

import android.database.SQLException;

import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Query;
import com.devexed.dalwit.Transaction;
import com.devexed.dalwit.UpdateStatement;

final class AndroidSQLiteUpdateStatement extends AndroidSQLiteStatementStatement implements UpdateStatement {

    public AndroidSQLiteUpdateStatement(AndroidSQLiteAbstractDatabase database, Query query) {
        super(database, query);
    }

    @Override
    public long update(Transaction transaction) {
        checkNotClosed();
        checkActiveTransaction(transaction);

        try {
            return statement.executeUpdateDelete();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
