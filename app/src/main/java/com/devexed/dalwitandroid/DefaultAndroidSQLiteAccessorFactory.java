package com.devexed.dalwitandroid;

import android.database.Cursor;
import android.database.SQLException;
import com.devexed.dalwit.AccessorFactory;
import com.devexed.dalwit.DatabaseException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * An accessor factory that matches the default JDBC accessor factory.
 */
public final class DefaultAndroidSQLiteAccessorFactory implements
        AccessorFactory<SQLiteBindable, android.database.Cursor, SQLException> {

    private static void checkBlobSize(long size) {
        if (size > 1024 * 1024)
            throw new DatabaseException(
                    "Attempted to bind a blob " + size + " bytes in size. " +
                    "Android's SQLite driver does not support blobs larger than 1MB.");
    }

    private static byte[] checkBlob(byte[] blob) {
        checkBlobSize(blob.length);

        return blob;
    }

    /**
     * Definitions for core java accessors that have a corresponding SQLite setter and getter.
     */
    private static final Map<Class<?>, AndroidSQLiteAccessor> accessors = new HashMap<Class<?>, AndroidSQLiteAccessor>() {{
        put(Boolean.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) throw new DatabaseException("Parameter with type boolean can not be null.");
                bindable.bindLong(index, (boolean) (Boolean) value ? 1 : 0);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index))
                    throw new DatabaseException("Illegal null value for type boolean in result set.");
                return cursor.getLong(index) != 0;
            }
        });
        put(Byte.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) throw new DatabaseException("Parameter with type byte can not be null.");
                bindable.bindLong(index, (Byte) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index))
                    throw new DatabaseException("Illegal null value for type byte in result set.");
                return (byte) cursor.getLong(index);
            }
        });
        put(Short.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) throw new DatabaseException("Parameter with type short can not be null.");
                bindable.bindLong(index, (Short) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index))
                    throw new DatabaseException("Illegal null value for type short in result set.");
                return (short) cursor.getLong(index);
            }
        });
        put(Integer.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) throw new DatabaseException("Parameter with type int can not be null.");
                bindable.bindLong(index, (Integer) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index))
                    throw new DatabaseException("Illegal null value for type int in result set.");
                return (int) cursor.getLong(index);
            }
        });
        put(Long.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) throw new DatabaseException("Parameter with type long can not be null.");
                bindable.bindLong(index, (Long) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index))
                    throw new DatabaseException("Illegal null value for type long in result set.");
                return cursor.getLong(index);
            }
        });
        put(Float.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) throw new DatabaseException("Parameter with type float can not be null.");
                bindable.bindDouble(index, (Float) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index))
                    throw new DatabaseException("Illegal null value for type float in result set.");
                return (float) cursor.getDouble(index);
            }
        });
        put(Double.TYPE, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) throw new DatabaseException("Parameter with type double can not be null.");
                bindable.bindDouble(index, (Double) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index))
                    throw new DatabaseException("Illegal null value for type double in result set.");
                return cursor.getDouble(index);
            }
        });
        put(Boolean.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, (boolean) (Boolean) value ? 1 : 0);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return cursor.getLong(index) != 0;
            }
        });
        put(Byte.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, (Byte) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return (byte) cursor.getLong(index);
            }
        });
        put(Short.class , new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, (Short) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return (short) cursor.getLong(index);
            }
        });
        put(Integer.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, (Integer) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return (int) cursor.getLong(index);
            }
        });
        put(Long.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, (Long) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return cursor.getLong(index);
            }
        });
        put(Float.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindDouble(index, (Float) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return (float) cursor.getDouble(index);
            }
        });
        put(Double.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindDouble(index, (Double) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return cursor.getDouble(index);
            }
        });
        put(String.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindString(index, (String) value);
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return cursor.getString(index);
            }
        });
        put(Date.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindLong(index, ((Date) value).getTime());
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return new Date(cursor.getLong(index));
            }
        });
        put(BigInteger.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindString(index, value.toString());
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return new BigInteger(cursor.getString(index));
            }
        });
        put(BigDecimal.class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindString(index, value.toString());
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return new BigDecimal(cursor.getString(index));
            }
        });
        put(byte[].class, new AndroidSQLiteAccessor() {
            @Override
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
                if (value == null) bindable.bindNull(index);
                else bindable.bindBlob(index, checkBlob((byte[]) value));
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return cursor.getBlob(index);
            }
        });
        put(InputStream.class, new AndroidSQLiteAccessor() {
            @Override
            @SuppressWarnings("TryFinallyCanBeTryWithResources")
            public void set(SQLiteBindable bindable, Integer index, Object value) throws SQLException {
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

                        while ((bytesRead = is.read(buffer)) != -1) {
                            os.write(buffer, 0, bytesRead);

                            // Check blob size on each write to fail early rather than after copying the whole stream.
                            checkBlobSize(os.size());
                        }
                    } finally {
                        os.close();
                    }
                } catch (IOException e) {
                    throw new DatabaseException("IO exception when writing input stream to blob", e);
                }

                bindable.bindBlob(index, checkBlob(os.toByteArray()));
            }

            @Override
            public Object get(Cursor cursor, Integer index) throws SQLException {
                if (cursor.isNull(index)) return null;
                else return new ByteArrayInputStream(cursor.getBlob(index));
            }
        });
    }};

    @Override
    public AndroidSQLiteAccessor create(Class<?> type) {
        return accessors.get(type);
    }

}
