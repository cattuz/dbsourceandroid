package com.devexed.dbsourceandroid;

final class AndroidSQLiteRootTransaction extends AndroidSQLiteTransaction {

    AndroidSQLiteRootTransaction(AndroidSQLiteAbstractDatabase parent) {
        super(parent);
    }

    @Override
    void beginTransaction() {
        connection.beginTransaction();
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
