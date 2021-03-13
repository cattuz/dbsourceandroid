package com.devexed.dalwitandroid;

import android.database.Cursor;
import android.database.SQLException;
import org.sqlite.database.sqlite.SQLiteDatabase;
import com.devexed.dalwit.AccessorFactory;
import com.devexed.dalwit.ColumnNameMapper;
import com.devexed.dalwit.Connection;
import com.devexed.dalwit.Database;
import com.devexed.dalwit.DefaultColumnNameMapper;
import com.devexed.dalwit.ReadonlyDatabase;

/**
 * A connection to an Android SQLite database.
 */
public final class AndroidSQLiteConnection implements Connection {

    private final String url;
    private final AccessorFactory<SQLiteBindable, android.database.Cursor, SQLException> accessorFactory;
    private final ColumnNameMapper columnNameMapper;

    public AndroidSQLiteConnection(String url, AccessorFactory<SQLiteBindable, Cursor, SQLException> accessorFactory, ColumnNameMapper columnNameMapper) {
        this.url = url;
        this.accessorFactory = accessorFactory;
        this.columnNameMapper = columnNameMapper;
    }

    public AndroidSQLiteConnection(String url) {
        this(url, new DefaultAndroidSQLiteAccessorFactory(), new DefaultColumnNameMapper());
    }

    private Database open(int flags) {
        return new AndroidSQLiteDatabase(SQLiteDatabase.openDatabase(url, null, flags), accessorFactory, columnNameMapper);
    }

    @Override
    public Database write() {
        return open(SQLiteDatabase.CREATE_IF_NECESSARY);
    }

    @Override
    public ReadonlyDatabase read() {
        return open(SQLiteDatabase.OPEN_READONLY);
    }

}
