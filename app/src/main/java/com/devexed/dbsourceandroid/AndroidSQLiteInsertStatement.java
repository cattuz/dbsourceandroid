package com.devexed.dbsourceandroid;

import android.database.SQLException;

import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.Cursors;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.EmptyCursor;
import com.devexed.dbsource.InsertStatement;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.Transaction;

import java.util.Map;

final class AndroidSQLiteInsertStatement extends AndroidSQLiteStatementStatement implements InsertStatement {

    final String key;

    public AndroidSQLiteInsertStatement(AndroidSQLiteAbstractDatabase database, Query query, Map<String, Class<?>> keys) {
        super(database, query, keys);

        if (keys.size() > 1)
            throw new DatabaseException("Only a single generated key column is supported.");

        key = keys.keySet().iterator().next();
        Class<?> type = keys.get(key);

        if (type != Long.TYPE) throw new DatabaseException("Generated key column must be of type Long.TYPE");
    }

    @Override
    public Cursor insert(Transaction transaction) {
        checkNotClosed();
        checkActiveTransaction(transaction);

        try {
            // Select last inserted id as key.
            final long generatedKey = statement.executeInsert();

            if (generatedKey < 0) return EmptyCursor.of();

            return Cursors.singleton(new Cursors.ColumnFunction() {
                @Override
                @SuppressWarnings("unchecked")
                public <E> E get(String column) {
                    if (!key.equals(column)) throw new DatabaseException("Column must be key column " + key);

                    return (E) (Long) generatedKey;
                }
            });
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
