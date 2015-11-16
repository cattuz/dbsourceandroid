package com.devexed.dbsourceandroid;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQuery;
import android.database.sqlite.SQLiteStatement;

import com.devexed.dbsource.Database;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;
import com.devexed.dbsource.TransactionDatabase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public final class AndroidSQLiteDatabase extends AndroidSQLiteAbstractDatabase {

    /**
     * Definitions for core java accessors that have a corresponding SQLite setter and getter.
     */
    public static final Map<Class<?>, AndroidSQLiteAccessor> accessors = new HashMap<Class<?>, AndroidSQLiteAccessor>() {{
        put(Boolean.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type boolean can not be null.");
                bindable.bindLong(index, (boolean) (Boolean) value ? 1 : 0);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index))
                    throw new NullPointerException("Illegal null value for type boolean in result set.");
                return cursor.getLong(index) != 0;
            }
        });
        put(Byte.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type byte can not be null.");
                bindable.bindLong(index, (Byte) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index))
                    throw new NullPointerException("Illegal null value for type byte in result set.");
                return (byte) cursor.getLong(index);
            }
        });
        put(Short.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type short can not be null.");
                bindable.bindLong(index, (Short) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index))
                    throw new NullPointerException("Illegal null value for type short in result set.");
                return (short) cursor.getLong(index);
            }
        });
        put(Integer.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type int can not be null.");
                bindable.bindLong(index, (Integer) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index))
                    throw new NullPointerException("Illegal null value for type int in result set.");
                return (int) cursor.getLong(index);
            }
        });
        put(Long.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type long can not be null.");
                bindable.bindLong(index, (Long) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index))
                    throw new NullPointerException("Illegal null value for type long in result set.");
                return cursor.getLong(index);
            }
        });
        put(Float.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type float can not be null.");
                bindable.bindDouble(index, (Float) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index))
                    throw new NullPointerException("Illegal null value for type float in result set.");
                return (float) cursor.getDouble(index);
            }
        });
        put(Double.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type double can not be null.");
                bindable.bindDouble(index, (Double) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index))
                    throw new NullPointerException("Illegal null value for type double in result set.");
                return cursor.getDouble(index);
            }
        });
        put(Boolean.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, (boolean) (Boolean) value ? 1 : 0);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return cursor.getLong(index) != 0;
            }
        });
        put(Byte.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, (Byte) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return (byte) cursor.getLong(index);
            }
        });
        put(Short.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, (Short) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return (short) cursor.getLong(index);
            }
        });
        put(Integer.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, (Integer) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return (int) cursor.getLong(index);
            }
        });
        put(Long.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, (Long) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return cursor.getLong(index);
            }
        });
        put(Float.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindDouble(index, (Float) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return (float) cursor.getDouble(index);
            }
        });
        put(Double.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindDouble(index, (Double) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return cursor.getDouble(index);
            }
        });
        put(String.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindString(index, (String) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return cursor.getString(index);
            }
        });
        put(Date.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, ((Date) value).getTime());
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return new Date(cursor.getLong(index));
            }
        });
        put(BigInteger.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindString(index, value.toString());
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return new BigInteger(cursor.getString(index));
            }
        });
        put(BigDecimal.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindString(index, value.toString());
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return new BigDecimal(cursor.getString(index));
            }
        });
        put(byte[].class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindBlob(index, (byte[]) value);
            }
            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return cursor.getBlob(index);
            }
        });
        put(InputStream.class, new AndroidSQLiteAccessor() {
            @Override
            @SuppressWarnings("TryFinallyCanBeTryWithResources")
            public void set(SQLiteBindable bindable, int index, Object value) throws SQLException {
                if (value == null) {
                    bindable.bindNull(index);
                    return;
                }

                ByteArrayOutputStream os = new ByteArrayOutputStream();
                InputStream is = (InputStream) value;

                try {
                    try {
                        byte[] buffer = new byte[1024];
                        int bytesRead;

                        while ((bytesRead = is.read(buffer)) != -1) os.write(buffer, 0, bytesRead);
                    } finally {
                        is.close();
                        os.close();
                    }
                } catch (IOException e) {
                    throw new DatabaseException(e);
                }

                bindable.bindBlob(index, os.toByteArray());
            }

            @Override
            public Object get(Cursor cursor, int index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return new ByteArrayInputStream(cursor.getBlob(index));
            }
        });
    }};

    public static Database openReadable(String url, Map<Class<?>, AndroidSQLiteAccessor> accessors) {
        return open(url, accessors, false);
    }

    public static TransactionDatabase openWritable(String url, Map<Class<?>, AndroidSQLiteAccessor> accessors) {
        return open(url, accessors, true);
    }

    private static void runPragma(SQLiteDatabase connection, String pragma) {
        // Runs a pragma on the database. Apparently needs to be a query whose cursor is actually used for the pragma to
        // take effect.
        Cursor cursor = connection.rawQuery(pragma, new String[0]);
        cursor.moveToFirst();
        cursor.close();
    }

	private static AndroidSQLiteDatabase open(String url, Map<Class<?>, AndroidSQLiteAccessor> accessors,
                                              boolean writable) {
		try {
            int flags = writable ? SQLiteDatabase.CREATE_IF_NECESSARY : SQLiteDatabase.OPEN_READONLY;
            SQLiteDatabase connection = SQLiteDatabase.openDatabase(url, null, flags);
            runPragma(connection, "PRAGMA foreign_keys=on");
            runPragma(connection, "PRAGMA journal_mode=delete");

			return new AndroidSQLiteDatabase(connection, accessors);
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

    private AndroidSQLiteDatabase(SQLiteDatabase connection, Map<Class<?>, AndroidSQLiteAccessor> accessors) {
		super(connection, accessors);
	}

	@Override
	public Transaction transact() {
		checkNotClosed();

		return new AndroidSQLiteRootTransaction(this);
	}

	@Override
	public void close() {
		checkNotClosed();

		try {
			connection.close();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

}
