/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.opendatabus;

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
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Bluetab
 *
 */
public class ResultSetWrapper implements ResultSet
{
    private ResultSet result;
    private HashMap<String, String> translate = new HashMap<String, String>();

    public String getSchema()
    {
        return schema;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    private String schema = "";

    public ResultSetWrapper(ResultSet result, HashMap<String, String> translate)
    {
        super();
        this.result = result;
        this.translate = translate;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        return result.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return result.isWrapperFor(iface);
    }

    public boolean next() throws SQLException
    {
        return result.next();
    }

    public void close() throws SQLException
    {
        result.close();
    }

    public boolean wasNull() throws SQLException
    {
        return result.wasNull();
    }

    public String getString(int columnIndex) throws SQLException
    {
        return result.getString(columnIndex);
    }

    public boolean getBoolean(int columnIndex) throws SQLException
    {
        return result.getBoolean(columnIndex);
    }

    public byte getByte(int columnIndex) throws SQLException
    {
        return result.getByte(columnIndex);
    }

    public short getShort(int columnIndex) throws SQLException
    {
        return result.getShort(columnIndex);
    }

    public int getInt(int columnIndex) throws SQLException
    {
        return result.getInt(columnIndex);
    }

    public long getLong(int columnIndex) throws SQLException
    {
        return result.getLong(columnIndex);
    }

    public float getFloat(int columnIndex) throws SQLException
    {
        return result.getFloat(columnIndex);
    }

    public double getDouble(int columnIndex) throws SQLException
    {
        return result.getDouble(columnIndex);
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
    {
        return result.getBigDecimal(columnIndex, scale);
    }

    public byte[] getBytes(int columnIndex) throws SQLException
    {
        return result.getBytes(columnIndex);
    }

    public Date getDate(int columnIndex) throws SQLException
    {
        return result.getDate(columnIndex);
    }

    public Time getTime(int columnIndex) throws SQLException
    {
        return result.getTime(columnIndex);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException
    {
        return result.getTimestamp(columnIndex);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException
    {
        return result.getAsciiStream(columnIndex);
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException
    {
        return result.getUnicodeStream(columnIndex);
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException
    {
        return result.getBinaryStream(columnIndex);
    }

    public String getString(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getString(columnLabel);
    }

    public boolean getBoolean(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getBoolean(columnLabel);
    }

    public byte getByte(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getByte(columnLabel);
    }

    public short getShort(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getShort(columnLabel);
    }

    public int getInt(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getInt(columnLabel);
    }

    public long getLong(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getLong(columnLabel);
    }

    public float getFloat(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getFloat(columnLabel);
    }

    public double getDouble(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getDouble(columnLabel);
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getBigDecimal(columnLabel, scale);
    }

    private String extracted(String columnLabel)
    {
        columnLabel = translate.get(columnLabel) != null ? translate.get(columnLabel) : columnLabel;
        return columnLabel;
    }

    public byte[] getBytes(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getBytes(columnLabel);
    }

    public Date getDate(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getDate(columnLabel);
    }

    public Time getTime(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getTime(columnLabel);
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getTimestamp(columnLabel);
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getAsciiStream(columnLabel);
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getUnicodeStream(columnLabel);
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getBinaryStream(columnLabel);
    }

    public SQLWarning getWarnings() throws SQLException
    {
        return result.getWarnings();
    }

    public void clearWarnings() throws SQLException
    {
        result.clearWarnings();
    }

    public String getCursorName() throws SQLException
    {
        return result.getCursorName();
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        return result.getMetaData();
    }

    public Object getObject(int columnIndex) throws SQLException
    {
        return result.getObject(columnIndex);
    }

    public Object getObject(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getObject(columnLabel);
    }

    public int findColumn(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.findColumn(columnLabel);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException
    {
        return result.getCharacterStream(columnIndex);
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getCharacterStream(columnLabel);
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        return result.getBigDecimal(columnIndex);
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getBigDecimal(columnLabel);
    }

    public boolean isBeforeFirst() throws SQLException
    {
        return result.isBeforeFirst();
    }

    public boolean isAfterLast() throws SQLException
    {
        return result.isAfterLast();
    }

    public boolean isFirst() throws SQLException
    {
        return result.isFirst();
    }

    public boolean isLast() throws SQLException
    {
        return result.isLast();
    }

    public void beforeFirst() throws SQLException
    {
        result.beforeFirst();
    }

    public void afterLast() throws SQLException
    {
        result.afterLast();
    }

    public boolean first() throws SQLException
    {
        return result.first();
    }

    public boolean last() throws SQLException
    {
        return result.last();
    }

    public int getRow() throws SQLException
    {
        return result.getRow();
    }

    public boolean absolute(int row) throws SQLException
    {
        return result.absolute(row);
    }

    public boolean relative(int rows) throws SQLException
    {
        return result.relative(rows);
    }

    public boolean previous() throws SQLException
    {
        return result.previous();
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        result.setFetchDirection(direction);
    }

    public int getFetchDirection() throws SQLException
    {
        return result.getFetchDirection();
    }

    public void setFetchSize(int rows) throws SQLException
    {
        result.setFetchSize(rows);
    }

    public int getFetchSize() throws SQLException
    {
        return result.getFetchSize();
    }

    public int getType() throws SQLException
    {
        return result.getType();
    }

    public int getConcurrency() throws SQLException
    {
        return result.getConcurrency();
    }

    public boolean rowUpdated() throws SQLException
    {
        return result.rowUpdated();
    }

    public boolean rowInserted() throws SQLException
    {
        return result.rowInserted();
    }

    public boolean rowDeleted() throws SQLException
    {
        return result.rowDeleted();
    }

    public void updateNull(int columnIndex) throws SQLException
    {
        result.updateNull(columnIndex);
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException
    {
        result.updateBoolean(columnIndex, x);
    }

    public void updateByte(int columnIndex, byte x) throws SQLException
    {
        result.updateByte(columnIndex, x);
    }

    public void updateShort(int columnIndex, short x) throws SQLException
    {
        result.updateShort(columnIndex, x);
    }

    public void updateInt(int columnIndex, int x) throws SQLException
    {
        result.updateInt(columnIndex, x);
    }

    public void updateLong(int columnIndex, long x) throws SQLException
    {
        result.updateLong(columnIndex, x);
    }

    public void updateFloat(int columnIndex, float x) throws SQLException
    {
        result.updateFloat(columnIndex, x);
    }

    public void updateDouble(int columnIndex, double x) throws SQLException
    {
        result.updateDouble(columnIndex, x);
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException
    {
        result.updateBigDecimal(columnIndex, x);
    }

    public void updateString(int columnIndex, String x) throws SQLException
    {
        result.updateString(columnIndex, x);
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException
    {
        result.updateBytes(columnIndex, x);
    }

    public void updateDate(int columnIndex, Date x) throws SQLException
    {
        result.updateDate(columnIndex, x);
    }

    public void updateTime(int columnIndex, Time x) throws SQLException
    {
        result.updateTime(columnIndex, x);
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
    {
        result.updateTimestamp(columnIndex, x);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException
    {
        result.updateAsciiStream(columnIndex, x, length);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException
    {
        result.updateBinaryStream(columnIndex, x, length);
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException
    {
        result.updateCharacterStream(columnIndex, x, length);
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException
    {
        result.updateObject(columnIndex, x, scaleOrLength);
    }

    public void updateObject(int columnIndex, Object x) throws SQLException
    {
        result.updateObject(columnIndex, x);
    }

    public void updateNull(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateNull(columnLabel);
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateBoolean(columnLabel, x);
    }

    public void updateByte(String columnLabel, byte x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateByte(columnLabel, x);
    }

    public void updateShort(String columnLabel, short x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateShort(columnLabel, x);
    }

    public void updateInt(String columnLabel, int x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateInt(columnLabel, x);
    }

    public void updateLong(String columnLabel, long x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateLong(columnLabel, x);
    }

    public void updateFloat(String columnLabel, float x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateFloat(columnLabel, x);
    }

    public void updateDouble(String columnLabel, double x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateDouble(columnLabel, x);
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateBigDecimal(columnLabel, x);
    }

    public void updateString(String columnLabel, String x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateString(columnLabel, x);
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateBytes(columnLabel, x);
    }

    public void updateDate(String columnLabel, Date x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateDate(columnLabel, x);
    }

    public void updateTime(String columnLabel, Time x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateTime(columnLabel, x);
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateTimestamp(columnLabel, x);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateAsciiStream(columnLabel, x, length);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateBinaryStream(columnLabel, x, length);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateCharacterStream(columnLabel, reader, length);
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateObject(columnLabel, x, scaleOrLength);
    }

    public void updateObject(String columnLabel, Object x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateObject(columnLabel, x);
    }

    public void insertRow() throws SQLException
    {
        result.insertRow();
    }

    public void updateRow() throws SQLException
    {
        result.updateRow();
    }

    public void deleteRow() throws SQLException
    {
        result.deleteRow();
    }

    public void refreshRow() throws SQLException
    {
        result.refreshRow();
    }

    public void cancelRowUpdates() throws SQLException
    {
        result.cancelRowUpdates();
    }

    public void moveToInsertRow() throws SQLException
    {
        result.moveToInsertRow();
    }

    public void moveToCurrentRow() throws SQLException
    {
        result.moveToCurrentRow();
    }

    public Statement getStatement() throws SQLException
    {
        return result.getStatement();
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException
    {
        return result.getObject(columnIndex, map);
    }

    public Ref getRef(int columnIndex) throws SQLException
    {
        return result.getRef(columnIndex);
    }

    public Blob getBlob(int columnIndex) throws SQLException
    {
        return result.getBlob(columnIndex);
    }

    public Clob getClob(int columnIndex) throws SQLException
    {
        return result.getClob(columnIndex);
    }

    public Array getArray(int columnIndex) throws SQLException
    {
        return result.getArray(columnIndex);
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getObject(columnLabel, map);
    }

    public Ref getRef(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getRef(columnLabel);
    }

    public Blob getBlob(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getBlob(columnLabel);
    }

    public Clob getClob(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getClob(columnLabel);
    }

    public Array getArray(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getArray(columnLabel);
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException
    {
        return result.getDate(columnIndex, cal);
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getDate(columnLabel, cal);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException
    {
        return result.getTime(columnIndex, cal);
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getTime(columnLabel, cal);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
    {
        return result.getTimestamp(columnIndex, cal);
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getTimestamp(columnLabel, cal);
    }

    public URL getURL(int columnIndex) throws SQLException
    {
        return result.getURL(columnIndex);
    }

    public URL getURL(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getURL(columnLabel);
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException
    {
        result.updateRef(columnIndex, x);
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateRef(columnLabel, x);
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException
    {
        result.updateBlob(columnIndex, x);
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateBlob(columnLabel, x);
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException
    {
        result.updateClob(columnIndex, x);
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateClob(columnLabel, x);
    }

    public void updateArray(int columnIndex, Array x) throws SQLException
    {
        result.updateArray(columnIndex, x);
    }

    public void updateArray(String columnLabel, Array x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateArray(columnLabel, x);
    }

    public RowId getRowId(int columnIndex) throws SQLException
    {
        return result.getRowId(columnIndex);
    }

    public RowId getRowId(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getRowId(columnLabel);
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException
    {
        result.updateRowId(columnIndex, x);
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateRowId(columnLabel, x);
    }

    public int getHoldability() throws SQLException
    {
        return result.getHoldability();
    }

    public boolean isClosed() throws SQLException
    {
        return result.isClosed();
    }

    public void updateNString(int columnIndex, String nString) throws SQLException
    {
        result.updateNString(columnIndex, nString);
    }

    public void updateNString(String columnLabel, String nString) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateNString(columnLabel, nString);
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException
    {
        result.updateNClob(columnIndex, nClob);
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateNClob(columnLabel, nClob);
    }

    public NClob getNClob(int columnIndex) throws SQLException
    {
        return result.getNClob(columnIndex);
    }

    public NClob getNClob(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getNClob(columnLabel);
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException
    {
        return result.getSQLXML(columnIndex);
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getSQLXML(columnLabel);
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException
    {
        result.updateSQLXML(columnIndex, xmlObject);
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateSQLXML(columnLabel, xmlObject);
    }

    public String getNString(int columnIndex) throws SQLException
    {
        return result.getNString(columnIndex);
    }

    public String getNString(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getNString(columnLabel);
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException
    {
        return result.getNCharacterStream(columnIndex);
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getNCharacterStream(columnLabel);
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException
    {
        result.updateNCharacterStream(columnIndex, x, length);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateNCharacterStream(columnLabel, reader, length);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException
    {
        result.updateAsciiStream(columnIndex, x, length);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException
    {
        result.updateBinaryStream(columnIndex, x, length);
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException
    {
        result.updateCharacterStream(columnIndex, x, length);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateAsciiStream(columnLabel, x, length);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateBinaryStream(columnLabel, x, length);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateCharacterStream(columnLabel, reader, length);
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException
    {
        result.updateBlob(columnIndex, inputStream, length);
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateBlob(columnLabel, inputStream, length);
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException
    {
        result.updateClob(columnIndex, reader, length);
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateClob(columnLabel, reader, length);
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException
    {
        result.updateNClob(columnIndex, reader, length);
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateNClob(columnLabel, reader, length);
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException
    {
        result.updateNCharacterStream(columnIndex, x);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateNCharacterStream(columnLabel, reader);
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException
    {
        result.updateAsciiStream(columnIndex, x);
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException
    {
        result.updateBinaryStream(columnIndex, x);
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException
    {
        result.updateCharacterStream(columnIndex, x);
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateAsciiStream(columnLabel, x);
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateBinaryStream(columnLabel, x);
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateCharacterStream(columnLabel, reader);
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException
    {
        result.updateBlob(columnIndex, inputStream);
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateBlob(columnLabel, inputStream);
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException
    {
        result.updateClob(columnIndex, reader);
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateClob(columnLabel, reader);
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException
    {
        result.updateNClob(columnIndex, reader);
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateNClob(columnLabel, reader);
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException
    {
        return result.getObject(columnIndex, type);
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        return result.getObject(columnLabel, type);
    }

    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException
    {
        result.updateObject(columnIndex, x, targetSqlType, scaleOrLength);
    }

    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
    }

    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException
    {
        result.updateObject(columnIndex, x, targetSqlType);
    }

    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException
    {
        columnLabel = extracted(columnLabel);
        result.updateObject(columnLabel, x, targetSqlType);
    }
}
