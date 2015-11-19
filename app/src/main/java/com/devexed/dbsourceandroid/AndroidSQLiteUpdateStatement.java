package com.devexed.dbsourceandroid;

import android.database.SQLException;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.Transaction;
import com.devexed.dbsource.UpdateStatement;

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
