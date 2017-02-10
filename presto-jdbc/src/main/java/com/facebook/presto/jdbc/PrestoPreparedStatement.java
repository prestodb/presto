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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.HashMap;

public class PrestoPreparedStatement
        extends PrestoStatement
        implements PreparedStatement
{
    /**
     * save the SQL parameters {paramLoc:paramValue}
     */
    private final HashMap<Integer, String> parameters = new HashMap<Integer, String>();
    private final String sql;
    PrestoPreparedStatement(PrestoConnection connection, String sql)
            throws SQLException
    {
        super(connection);
        this.sql = sql;
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException
    {
        return super.executeQuery(updateSql(this.sql, parameters));
    }

    @Override
    public int executeUpdate()
            throws SQLException
    {
        if (!super.execute(updateSql(sql, parameters))) {
            throw new SQLException("SQL statement is not a query: " + sql);
        }
        return (int) this.getLargeUpdateCount();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType)
            throws SQLException
    {
        this.parameters.put(parameterIndex, "NULL");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x)
            throws SQLException
    {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setByte(int parameterIndex, byte x)
            throws SQLException
    {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setShort(int parameterIndex, short x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setShort");
    }

    @Override
    public void setInt(int parameterIndex, int x)
            throws SQLException
    {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setLong(int parameterIndex, long x)
            throws SQLException
    {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setFloat(int parameterIndex, float x)
            throws SQLException
    {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setDouble(int parameterIndex, double x)
            throws SQLException
    {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBigDecimal");
    }

    @Override
    public void setString(int parameterIndex, String x)
            throws SQLException
    {
        x = x.replace("'", "\\'");
        this.parameters.put(parameterIndex, "'" + x + "'");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBytes");
    }

    @Override
    public void setDate(int parameterIndex, Date x)
            throws SQLException
    {
        this.parameters.put(parameterIndex, "'" + x.toString() + "'");
    }

    @Override
    public void setTime(int parameterIndex, Time x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException
    {
        this.parameters.put(parameterIndex, "'" + x.toString() + "'");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setAsciiStream");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBinaryStream");
    }

    @Override
    public void clearParameters()
            throws SQLException
    {
        this.parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setObject");
    }

    @Override
    public void setObject(int parameterIndex, Object x)
            throws SQLException
    {
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        }
        else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        }
        else if (x instanceof Short) {
            setShort(parameterIndex, ((Short) x).shortValue());
        }
        else if (x instanceof Integer) {
            setInt(parameterIndex, ((Integer) x).intValue());
        }
        else if (x instanceof Long) {
            setLong(parameterIndex, ((Long) x).longValue());
        }
        else if (x instanceof Float) {
            setFloat(parameterIndex, ((Float) x).floatValue());
        }
        else if (x instanceof Double) {
            setDouble(parameterIndex, ((Double) x).doubleValue());
        }
        else if (x instanceof Boolean) {
            setBoolean(parameterIndex, ((Boolean) x).booleanValue());
        }
        else if (x instanceof Byte) {
            setByte(parameterIndex, ((Byte) x).byteValue());
        }
        else if (x instanceof Character) {
            setString(parameterIndex, x.toString());
        }
        else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        }
        else if (x instanceof BigDecimal) {
            setString(parameterIndex, x.toString());
        }
        else {
            // Can't infer a type.
            throw new SQLException(
                    MessageFormat
                            .format(
                                    "Can''t infer the SQL type to use for an instance of {0}. Use setObject() with an explicit Types value to specify the type to use.",
                                    x.getClass().getName()));
        }
    }

    @Override
    public boolean execute()
            throws SQLException
    {
        return super.execute(updateSql(sql, parameters));
    }

    @Override
    public void addBatch()
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "addBatch");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setRef(int parameterIndex, Ref x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setRef");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setClob(int parameterIndex, Clob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setArray(int parameterIndex, Array x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setArray");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getMetaData");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setTimestamp");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException
    {
        this.parameters.put(parameterIndex, "NULL");
    }

    @Override
    public void setURL(int parameterIndex, URL x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setURL");
    }

    @Override
    public ParameterMetaData getParameterMetaData()
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "getParameterMetaData");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setRowId");
    }

    @Override
    public void setNString(int parameterIndex, String value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNString");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNCharacterStream");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setSQLXML");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setObject");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setCharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNCharacterStream");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void addBatch(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    /**
     * update the SQL string with parameters set by setXXX methods of {@link PreparedStatement}
     *
     * @param sql
     * @param parameters
     * @return updated SQL string
     */
    private String updateSql(final String sql, HashMap<Integer, String> parameters)
    {
        if (!sql.contains("?")) {
            return sql;
        }

        StringBuilder newSql = new StringBuilder(sql);

        int paramLoc = 1;
        while (getCharIndexFromSqlByParamLocation(sql, '?', paramLoc) > 0) {
            // check the user has set the needs parameters
            if (parameters.containsKey(paramLoc)) {
                int tt = getCharIndexFromSqlByParamLocation(newSql.toString(), '?', 1);
                newSql.deleteCharAt(tt);
                newSql.insert(tt, parameters.get(paramLoc));
            }
            paramLoc++;
        }
        System.out.println(newSql.toString());
        return newSql.toString();
    }

    /**
     * Get the index of given char from the SQL string by parameter location
     * </br> The -1 will be return, if nothing found
     *
     * @param sql
     * @param cchar
     * @param paramLoc
     * @return
     */
    private int getCharIndexFromSqlByParamLocation(final String sql, final char cchar, final int paramLoc)
    {
        int signalCount = 0;
        int charIndex = -1;
        int num = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '\\') {
                signalCount++;
            }
            else if (c == cchar && signalCount % 2 == 0) {
                // check if the ? is really the parameter
                num++;
                if (num == paramLoc) {
                    charIndex = i;
                    break;
                }
            }
        }
        return charIndex;
    }
}
