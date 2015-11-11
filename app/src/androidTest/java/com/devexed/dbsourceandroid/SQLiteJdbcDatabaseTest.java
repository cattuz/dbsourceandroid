package com.devexed.dbsourceandroid;

import com.devexed.dbsource.DatabaseTest;
import com.devexed.dbsource.TransactionDatabase;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public final class SQLiteJdbcDatabaseTest extends DatabaseTest {

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Override
    public TransactionDatabase openDatabase() {
        String dbPath = new File(dbFolder.getRoot(), "test.android-sqlite.db").getAbsolutePath();

        return AndroidSQLiteDatabase.openWritable(dbPath, AndroidSQLiteDatabase.accessors);
    }

}
