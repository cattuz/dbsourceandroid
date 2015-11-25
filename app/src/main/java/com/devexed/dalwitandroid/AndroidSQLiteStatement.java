package com.devexed.dalwitandroid;

import com.devexed.dalwit.Query;
import com.devexed.dalwit.Statement;
import com.devexed.dalwit.util.AbstractCloseable;

abstract class AndroidSQLiteStatement extends AbstractCloseable implements Statement {

    final AndroidSQLiteAbstractDatabase database;
    final Query query;

    AndroidSQLiteStatement(AndroidSQLiteAbstractDatabase database, Query query) {
        this.database = database;
        this.query = query;
    }

}
