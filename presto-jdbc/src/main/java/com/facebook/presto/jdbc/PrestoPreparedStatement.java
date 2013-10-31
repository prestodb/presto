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
package com.facebook.presto.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class PrestoPreparedStatement
        extends PrestoStatement
        implements PreparedStatement
{
    PrestoPreparedStatement(PrestoConnection connection, String sql)
    {
        super(connection);
        throw new UnsupportedOperationException("PreparedStatement");
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException
    {
        throw new UnsupportedOperationException("executeQuery");
    }

    @Override
    public int executeUpdate()
            throws SQLException
    {
        throw new UnsupportedOperationException("executeUpdate");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType)
            throws SQLException
    {
        throw new UnsupportedOperationException("setNull");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setBoolean");
    }

    @Override
    public void setByte(int parameterIndex, byte x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setByte");
    }

    @Override
    public void setShort(int parameterIndex, short x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setShort");
    }

    @Override
    public void setInt(int parameterIndex, int x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setInt");
    }

    @Override
    public void setLong(int parameterIndex, long x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setLong");
    }

    @Override
    public void setFloat(int parameterIndex, float x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setFloat");
    }

    @Override
    public void setDouble(int parameterIndex, double x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setDouble");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setBigDecimal");
    }

    @Override
    public void setString(int parameterIndex, String x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setString");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setBytes");
    }

    @Override
    public void setDate(int parameterIndex, Date x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setTimestamp");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setAsciiStream");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setBinaryStream");
    }

    @Override
    public void clearParameters()
            throws SQLException
    {
        throw new UnsupportedOperationException("clearParameters");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException
    {
        throw new UnsupportedOperationException("setObject");
    }

    @Override
    public void setObject(int parameterIndex, Object x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setObject");
    }

    @Override
    public boolean execute()
            throws SQLException
    {
        throw new UnsupportedOperationException("execute");
    }

    @Override
    public void addBatch()
            throws SQLException
    {
        throw new UnsupportedOperationException("addBatch");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setCharacterStream");
    }

    @Override
    public void setRef(int parameterIndex, Ref x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setRef");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setBlob");
    }

    @Override
    public void setClob(int parameterIndex, Clob x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setClob");
    }

    @Override
    public void setArray(int parameterIndex, Array x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setArray");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        throw new UnsupportedOperationException("getMetaData");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("setTimestamp");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException
    {
        throw new UnsupportedOperationException("setNull");
    }

    @Override
    public void setURL(int parameterIndex, URL x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setURL");
    }

    @Override
    public ParameterMetaData getParameterMetaData()
            throws SQLException
    {
        throw new UnsupportedOperationException("getParameterMetaData");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setRowId");
    }

    @Override
    public void setNString(int parameterIndex, String value)
            throws SQLException
    {
        throw new UnsupportedOperationException("setNString");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setNCharacterStream");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value)
            throws SQLException
    {
        throw new UnsupportedOperationException("setNClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setNClob");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new UnsupportedOperationException("setSQLXML");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException
    {
        throw new UnsupportedOperationException("setObject");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("setCharacterStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new UnsupportedOperationException("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("setCharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException
    {
        throw new UnsupportedOperationException("setNCharacterStream");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException
    {
        throw new UnsupportedOperationException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("setNClob");
    }

    @Override
    public void addBatch(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }
}
