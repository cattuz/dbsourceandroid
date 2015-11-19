package com.devexed.dbsourceandroid;

import android.database.SQLException;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.ExecutionStatement;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.Transaction;

final class AndroidSQLiteExecutionStatement extends AndroidSQLiteStatementStatement implements ExecutionStatement {

    public AndroidSQLiteExecutionStatement(AndroidSQLiteAbstractDatabase database, Query query) {
        super(database, query);
    }

    @Override
    public void execute(Transaction transaction) {
        checkNotClosed();
        checkActiveTransaction(transaction);

        try {
            statement.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
