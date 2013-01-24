package com.facebook.presto.jdbc;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.server.HttpQueryClient;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.cli.Query.getFailureMessages;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JdbcResultSet
        implements ResultSet
{
    private static final int VARIABLE_BINARY_MAX = 1024 * 1024 * 1024;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final HttpQueryClient queryClient;
    private final ResultsIterator results;
    private final Map<String, Integer> fieldMap;
    private final ResultSetMetaData resultSetMetaData;
    private final AtomicReference<List<Object>> row = new AtomicReference<>();
    private final AtomicBoolean wasNull = new AtomicBoolean();

    JdbcResultSet(HttpQueryClient queryClient)
            throws SQLException
    {
        this.queryClient = checkNotNull(queryClient, "queryClient is null");

        QueryInfo queryInfo = waitForResults();
        Operator operator = queryClient.getResultsOperator();

        results = new ResultsIterator(operator);

        List<String> fieldNames = ImmutableList.copyOf(queryInfo.getFieldNames());
        List<TupleInfo.Type> fieldTypes = getFieldTypes(operator.getTupleInfos());

        fieldMap = getFieldMap(fieldNames);
        resultSetMetaData = new JdbcResultSetMetaData(getColumnInfo(fieldNames, fieldTypes));
    }

    @Override
    public boolean next()
            throws SQLException
    {
        checkOpen();
        if (!results.hasNext()) {
            row.set(null);
            return false;
        }
        row.set(results.next());
        return true;
    }

    @Override
    public void close()
            throws SQLException
    {
        if (!closed.getAndSet(true)) {
            queryClient.cancelQuery();
        }
    }

    @Override
    public boolean wasNull()
            throws SQLException
    {
        return wasNull.get();
    }

    @Override
    public String getString(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBoolean");
    }

    @Override
    public byte getByte(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).byteValue() : 0;
    }

    @Override
    public short getShort(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).shortValue() : 0;
    }

    @Override
    public int getInt(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).intValue() : 0;
    }

    @Override
    public long getLong(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).longValue() : 0;
    }

    @Override
    public float getFloat(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).floatValue() : 0;
    }

    @Override
    public double getDouble(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).doubleValue() : 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBigDecimal");
    }

    @Override
    public byte[] getBytes(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBytes");
    }

    @Override
    public Date getDate(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getDate");
    }

    @Override
    public Time getTime(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getTime");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getTimestamp");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getAsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBinaryStream");
    }

    @Override
    public String getString(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBoolean");
    }

    @Override
    public byte getByte(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).byteValue() : 0;
    }

    @Override
    public short getShort(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).shortValue() : 0;
    }

    @Override
    public int getInt(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).intValue() : 0;
    }

    @Override
    public long getLong(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).longValue() : 0;
    }

    @Override
    public float getFloat(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).floatValue() : 0;
    }

    @Override
    public double getDouble(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).doubleValue() : 0;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBigDecimal");
    }

    @Override
    public byte[] getBytes(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBytes");
    }

    @Override
    public Date getDate(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getDate");
    }

    @Override
    public Time getTime(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getTime");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getTimestamp");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getAsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBinaryStream");
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
        checkOpen();
    }

    @Override
    public String getCursorName()
            throws SQLException
    {
        throw new UnsupportedOperationException("getCursorName");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        return resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex)
            throws SQLException
    {
        return column(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel)
            throws SQLException
    {
        return column(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel)
            throws SQLException
    {
        checkOpen();
        return columnIndex(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getCharacterStream");
    }

    @Override
    public Reader getCharacterStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getCharacterStream");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBigDecimal");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBigDecimal");
    }

    @Override
    public boolean isBeforeFirst()
            throws SQLException
    {
        throw new UnsupportedOperationException("isBeforeFirst");
    }

    @Override
    public boolean isAfterLast()
            throws SQLException
    {
        throw new UnsupportedOperationException("isAfterLast");
    }

    @Override
    public boolean isFirst()
            throws SQLException
    {
        throw new UnsupportedOperationException("isFirst");
    }

    @Override
    public boolean isLast()
            throws SQLException
    {
        throw new UnsupportedOperationException("isLast");
    }

    @Override
    public void beforeFirst()
            throws SQLException
    {
        throw new UnsupportedOperationException("beforeFirst");
    }

    @Override
    public void afterLast()
            throws SQLException
    {
        throw new UnsupportedOperationException("afterLast");
    }

    @Override
    public boolean first()
            throws SQLException
    {
        throw new UnsupportedOperationException("first");
    }

    @Override
    public boolean last()
            throws SQLException
    {
        throw new UnsupportedOperationException("last");
    }

    @Override
    public int getRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("getRow");
    }

    @Override
    public boolean absolute(int row)
            throws SQLException
    {
        throw new UnsupportedOperationException("absolute");
    }

    @Override
    public boolean relative(int rows)
            throws SQLException
    {
        throw new UnsupportedOperationException("relative");
    }

    @Override
    public boolean previous()
            throws SQLException
    {
        throw new UnsupportedOperationException("previous");
    }

    @Override
    public void setFetchDirection(int direction)
            throws SQLException
    {
        throw new UnsupportedOperationException("setFetchDirection");
    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        throw new UnsupportedOperationException("getFetchDirection");
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
        throw new UnsupportedOperationException("setFetchSize");
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        throw new UnsupportedOperationException("getFetchSize");
    }

    @Override
    public int getType()
            throws SQLException
    {
        throw new UnsupportedOperationException("getType");
    }

    @Override
    public int getConcurrency()
            throws SQLException
    {
        throw new UnsupportedOperationException("getConcurrency");
    }

    @Override
    public boolean rowUpdated()
            throws SQLException
    {
        throw new UnsupportedOperationException("rowUpdated");
    }

    @Override
    public boolean rowInserted()
            throws SQLException
    {
        throw new UnsupportedOperationException("rowInserted");
    }

    @Override
    public boolean rowDeleted()
            throws SQLException
    {
        throw new UnsupportedOperationException("rowDeleted");
    }

    @Override
    public void updateNull(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNull");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBoolean");
    }

    @Override
    public void updateByte(int columnIndex, byte x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateByte");
    }

    @Override
    public void updateShort(int columnIndex, short x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateShort");
    }

    @Override
    public void updateInt(int columnIndex, int x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateInt");
    }

    @Override
    public void updateLong(int columnIndex, long x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateLong");
    }

    @Override
    public void updateFloat(int columnIndex, float x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateFloat");
    }

    @Override
    public void updateDouble(int columnIndex, double x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateDouble");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBigDecimal");
    }

    @Override
    public void updateString(int columnIndex, String x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateString");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBytes");
    }

    @Override
    public void updateDate(int columnIndex, Date x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateDate");
    }

    @Override
    public void updateTime(int columnIndex, Time x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateTime");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateObject");
    }

    @Override
    public void updateNull(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNull");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBoolean");
    }

    @Override
    public void updateByte(String columnLabel, byte x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateByte");
    }

    @Override
    public void updateShort(String columnLabel, short x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateShort");
    }

    @Override
    public void updateInt(String columnLabel, int x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateInt");
    }

    @Override
    public void updateLong(String columnLabel, long x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateLong");
    }

    @Override
    public void updateFloat(String columnLabel, float x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateFloat");
    }

    @Override
    public void updateDouble(String columnLabel, double x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateDouble");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBigDecimal");
    }

    @Override
    public void updateString(String columnLabel, String x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateString");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBytes");
    }

    @Override
    public void updateDate(String columnLabel, Date x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateDate");
    }

    @Override
    public void updateTime(String columnLabel, Time x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateTime");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateObject");
    }

    @Override
    public void insertRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("insertRow");
    }

    @Override
    public void updateRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("updateRow");
    }

    @Override
    public void deleteRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("deleteRow");
    }

    @Override
    public void refreshRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("refreshRow");
    }

    @Override
    public void cancelRowUpdates()
            throws SQLException
    {
        throw new UnsupportedOperationException("cancelRowUpdates");
    }

    @Override
    public void moveToInsertRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("moveToCurrentRow");
    }

    @Override
    public Statement getStatement()
            throws SQLException
    {
        throw new UnsupportedOperationException("getStatement");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new UnsupportedOperationException("getObject");
    }

    @Override
    public Ref getRef(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getRef");
    }

    @Override
    public Blob getBlob(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBlob");
    }

    @Override
    public Clob getClob(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getClob");
    }

    @Override
    public Array getArray(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getArray");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new UnsupportedOperationException("getObject");
    }

    @Override
    public Ref getRef(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getRef");
    }

    @Override
    public Blob getBlob(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBlob");
    }

    @Override
    public Clob getClob(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getClob");
    }

    @Override
    public Array getArray(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getArray");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("getDate");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("getDate");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("getTime");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("getTime");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("getTimestamp");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("getTimestamp");
    }

    @Override
    public URL getURL(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getURL");
    }

    @Override
    public URL getURL(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getURL");
    }

    @Override
    public void updateRef(int columnIndex, Ref x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateRef");
    }

    @Override
    public void updateRef(String columnLabel, Ref x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateRef");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Clob x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Clob x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateArray(int columnIndex, Array x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateArray");
    }

    @Override
    public void updateArray(String columnLabel, Array x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateArray");
    }

    @Override
    public RowId getRowId(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getRowId");
    }

    @Override
    public RowId getRowId(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getRowId");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateRowId");
    }

    @Override
    public int getHoldability()
            throws SQLException
    {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return closed.get();
    }

    @Override
    public void updateNString(int columnIndex, String nString)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNString");
    }

    @Override
    public void updateNString(String columnLabel, String nString)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNString");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public NClob getNClob(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNClob");
    }

    @Override
    public NClob getNClob(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNClob");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getSQLXML");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateSQLXML");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateSQLXML");
    }

    @Override
    public String getNString(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNString");
    }

    @Override
    public String getNString(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNString");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type)
            throws SQLException
    {
        throw new UnsupportedOperationException("getObject");
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException
    {
        throw new UnsupportedOperationException("getObject");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return iface.isInstance(this);
    }

    private void checkOpen()
            throws SQLException
    {
        if (isClosed()) {
            throw new SQLException("ResultSet is closed");
        }
    }

    private void checkValidRow()
            throws SQLException
    {
        if (row.get() == null) {
            throw new SQLException("Not on a valid row");
        }
    }

    private Object column(int index)
            throws SQLException
    {
        checkOpen();
        checkValidRow();
        if ((index <= 0) || (index > fieldMap.size())) {
            throw new SQLException("Invalid column index: " + index);
        }
        return row.get().get(index - 1);
    }

    private Object column(String label)
            throws SQLException
    {
        checkOpen();
        checkValidRow();
        return row.get().get(columnIndex(label) - 1);
    }

    private int columnIndex(String label)
            throws SQLException
    {
        if (label == null) {
            throw new SQLException("Column label is null");
        }
        Integer index = fieldMap.get(label.toLowerCase());
        if (index == null) {
            throw new SQLException("Invalid column label: " + label);
        }
        return index;
    }

    private QueryInfo waitForResults()
            throws SQLException
    {
        QueryInfo queryInfo = waitForQuery();
        if (queryInfo == null) {
            throw new SQLException("Query is gone (server restarted?)");
        }
        if (queryInfo.getState().isDone()) {
            switch (queryInfo.getState()) {
                case CANCELED:
                    throw new SQLException(format("Query was canceled (#%s)", queryInfo.getQueryId()));
                case FAILED:
                    throw new SQLException(failureMessage(queryInfo));
                default:
                    throw new SQLException("Query finished with no output (#%s)", queryInfo.getQueryId());
            }
        }
        return queryInfo;
    }

    private QueryInfo waitForQuery()
            throws SQLException
    {
        int errors = 0;
        while (true) {
            try {
                QueryInfo queryInfo = queryClient.getQueryInfo(false);

                // if query is no longer running, finish
                if ((queryInfo == null) || queryInfo.getState().isDone()) {
                    return queryInfo;
                }

                // check if there is there is pending output
                if (queryInfo.resultsPending()) {
                    return queryInfo;
                }

                // TODO: add a blocking method on server
                Uninterruptibles.sleepUninterruptibly(100, MILLISECONDS);
            }
            catch (Exception e) {
                errors++;
                if (errors > 10) {
                    throw new SQLException("Error waiting for query results", e);
                }
            }
        }
    }

    private static String failureMessage(QueryInfo queryInfo)
    {
        Set<String> failureMessages = ImmutableSet.copyOf(getFailureMessages(queryInfo));
        if (failureMessages.isEmpty()) {
            return format("Query failed for an unknown reason (#%s)", queryInfo.getQueryId());
        }
        return format("Query failed (#%s): %s", queryInfo.getQueryId(), Joiner.on("; ").join(failureMessages));
    }

    private static Map<String, Integer> getFieldMap(List<String> fieldNames)
    {
        ImmutableMap.Builder<String, Integer> map = ImmutableMap.builder();
        for (int i = 0; i < fieldNames.size(); i++) {
            map.put(fieldNames.get(i).toLowerCase(), i + 1);
        }
        return map.build();
    }

    private static List<TupleInfo.Type> getFieldTypes(List<TupleInfo> tupleInfos)
    {
        ImmutableList.Builder<TupleInfo.Type> list = ImmutableList.builder();
        for (TupleInfo tupleInfo : tupleInfos) {
            list.addAll(tupleInfo.getTypes());
        }
        return list.build();
    }

    private static List<ColumnInfo> getColumnInfo(List<String> fieldNames, List<TupleInfo.Type> fieldTypes)
    {
        checkArgument(fieldNames.size() == fieldTypes.size(), "names / types size mismatch");
        ImmutableList.Builder<ColumnInfo> list = ImmutableList.builder();
        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            TupleInfo.Type type = fieldTypes.get(i);
            ColumnInfo.Builder builder = new ColumnInfo.Builder()
                    .setCatalogName("") // TODO
                    .setSchemaName("") // TODO
                    .setTableName("") // TODO
                    .setColumnLabel(name)
                    .setColumnName(name) // TODO
                    .setColumnTypeName(type.getName().toUpperCase())
                    .setNullable(ResultSetMetaData.columnNullableUnknown)
                    .setCurrency(false);
            setTypeInfo(builder, type);
            list.add(builder.build());
        }
        return list.build();
    }

    private static void setTypeInfo(ColumnInfo.Builder builder, TupleInfo.Type type)
    {
        switch (type) {
            case FIXED_INT_64:
                builder.setColumnType(Types.BIGINT);
                builder.setSigned(true);
                builder.setPrecision(19);
                builder.setScale(0);
                builder.setColumnDisplaySize(20);
                break;
            case DOUBLE:
                builder.setColumnType(Types.DOUBLE);
                builder.setSigned(true);
                builder.setPrecision(17);
                builder.setScale(0);
                builder.setColumnDisplaySize(24);
                break;
            case VARIABLE_BINARY:
                builder.setColumnType(Types.LONGNVARCHAR);
                builder.setSigned(true);
                builder.setPrecision(VARIABLE_BINARY_MAX);
                builder.setScale(0);
                builder.setColumnDisplaySize(VARIABLE_BINARY_MAX);
                break;
            default:
                throw new AssertionError("unimplemented type: " + type);
        }
    }
}
