package com.devexed.dbsourceandroid;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;

import com.devexed.dbsource.AccessorFactory;
import com.devexed.dbsource.Connection;
import com.devexed.dbsource.Database;
import com.devexed.dbsource.ReadonlyDatabase;
import com.devexed.dbsource.util.CloseableManager;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A connection to an Android SQLite database.
 */
public final class AndroidSQLiteConnection implements Connection {

    private final String url;
    private final AccessorFactory<SQLiteBindable, Cursor, SQLException> accessorFactory;
    private final CloseableManager<AndroidSQLiteDatabase> databaseManager =
            new CloseableManager<AndroidSQLiteDatabase>(Connection.class, Database.class,
                    Collections.newSetFromMap(new ConcurrentHashMap<AndroidSQLiteDatabase, Boolean>()));

    public AndroidSQLiteConnection(String url, AccessorFactory<SQLiteBindable, Cursor, SQLException> accessorFactory) {
        this.url = url;
        this.accessorFactory = accessorFactory;
    }

    public AndroidSQLiteConnection(String url) {
        this(url, new DefaultAndroidSQLiteAccessorFactory());
    }

    private Database open(int flags) {
        return databaseManager.open(new AndroidSQLiteDatabase(SQLiteDatabase.openDatabase(url, null, flags),
                accessorFactory));
    }

    @Override
    public Database write() {
        return open(SQLiteDatabase.CREATE_IF_NECESSARY);
    }

    @Override
    public ReadonlyDatabase read() {
        return open(SQLiteDatabase.OPEN_READONLY);
    }

    @Override
    public void close(ReadonlyDatabase database) {
        databaseManager.close(database);
    }

}
