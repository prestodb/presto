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
package io.prestosql.jdbc;

import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;

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
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.io.BaseEncoding.base16;
import static io.prestosql.jdbc.ObjectCasts.castToBigDecimal;
import static io.prestosql.jdbc.ObjectCasts.castToBinary;
import static io.prestosql.jdbc.ObjectCasts.castToBoolean;
import static io.prestosql.jdbc.ObjectCasts.castToByte;
import static io.prestosql.jdbc.ObjectCasts.castToDate;
import static io.prestosql.jdbc.ObjectCasts.castToDouble;
import static io.prestosql.jdbc.ObjectCasts.castToFloat;
import static io.prestosql.jdbc.ObjectCasts.castToInt;
import static io.prestosql.jdbc.ObjectCasts.castToLong;
import static io.prestosql.jdbc.ObjectCasts.castToShort;
import static io.prestosql.jdbc.ObjectCasts.castToTime;
import static io.prestosql.jdbc.ObjectCasts.castToTimestamp;
import static io.prestosql.jdbc.PrestoResultSet.DATE_FORMATTER;
import static io.prestosql.jdbc.PrestoResultSet.TIMESTAMP_FORMATTER;
import static io.prestosql.jdbc.PrestoResultSet.TIME_FORMATTER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoPreparedStatement
        extends PrestoStatement
        implements PreparedStatement
{
    private final Map<Integer, String> parameters = new HashMap<>();
    private final String statementName;
    private final String originalSql;

    PrestoPreparedStatement(PrestoConnection connection, String statementName, String sql)
            throws SQLException
    {
        super(connection);
        this.statementName = requireNonNull(statementName, "statementName is null");
        this.originalSql = requireNonNull(sql, "sql is null");
        super.execute(format("PREPARE %s FROM %s", statementName, sql));
    }

    @Override
    public void close()
            throws SQLException
    {
        super.execute(format("DEALLOCATE PREPARE %s", statementName));
        super.close();
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException
    {
        if (!super.execute(getExecuteSql())) {
            throw new SQLException("Prepared SQL statement is not a query: " + originalSql);
        }
        return getResultSet();
    }

    @Override
    public int executeUpdate()
            throws SQLException
    {
        return Ints.saturatedCast(executeLargeUpdate());
    }

    @Override
    public long executeLargeUpdate()
            throws SQLException
    {
        if (super.execute(getExecuteSql())) {
            throw new SQLException("Prepared SQL is not an update statement: " + originalSql);
        }
        return getLargeUpdateCount();
    }

    @Override
    public boolean execute()
            throws SQLException
    {
        return super.execute(getExecuteSql());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, typedNull(sqlType));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatBooleanLiteral(x));
    }

    @Override
    public void setByte(int parameterIndex, byte x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("TINYINT", Byte.toString(x)));
    }

    @Override
    public void setShort(int parameterIndex, short x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("SMALLINT", Short.toString(x)));
    }

    @Override
    public void setInt(int parameterIndex, int x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("INTEGER", Integer.toString(x)));
    }

    @Override
    public void setLong(int parameterIndex, long x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("BIGINT", Long.toString(x)));
    }

    @Override
    public void setFloat(int parameterIndex, float x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("REAL", Float.toString(x)));
    }

    @Override
    public void setDouble(int parameterIndex, double x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("DOUBLE", Double.toString(x)));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.DECIMAL);
        }
        else {
            setParameter(parameterIndex, formatLiteral("DECIMAL", x.toString()));
        }
    }

    @Override
    public void setString(int parameterIndex, String x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.VARCHAR);
        }
        else {
            setParameter(parameterIndex, formatStringLiteral(x));
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.VARBINARY);
        }
        else {
            setParameter(parameterIndex, formatBinaryLiteral(x));
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.DATE);
        }
        else {
            setParameter(parameterIndex, formatLiteral("DATE", DATE_FORMATTER.print(x.getTime())));
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.TIME);
        }
        else {
            setParameter(parameterIndex, formatLiteral("TIME", TIME_FORMATTER.print(x.getTime())));
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
        }
        else {
            setParameter(parameterIndex, formatLiteral("TIMESTAMP", TIMESTAMP_FORMATTER.print(x.getTime())));
        }
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
        checkOpen();
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
            return;
        }
        switch (targetSqlType) {
            case Types.BOOLEAN:
            case Types.BIT:
                setBoolean(parameterIndex, castToBoolean(x, targetSqlType));
                return;
            case Types.TINYINT:
                setByte(parameterIndex, castToByte(x, targetSqlType));
                return;
            case Types.SMALLINT:
                setShort(parameterIndex, castToShort(x, targetSqlType));
                return;
            case Types.INTEGER:
                setInt(parameterIndex, castToInt(x, targetSqlType));
                return;
            case Types.BIGINT:
                setLong(parameterIndex, castToLong(x, targetSqlType));
                return;
            case Types.FLOAT:
            case Types.REAL:
                setFloat(parameterIndex, castToFloat(x, targetSqlType));
                return;
            case Types.DOUBLE:
                setDouble(parameterIndex, castToDouble(x, targetSqlType));
                return;
            case Types.DECIMAL:
            case Types.NUMERIC:
                setBigDecimal(parameterIndex, castToBigDecimal(x, targetSqlType));
                return;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                setString(parameterIndex, x.toString());
                return;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                setBytes(parameterIndex, castToBinary(x, targetSqlType));
                return;
            case Types.DATE:
                setDate(parameterIndex, castToDate(x, targetSqlType));
                return;
            case Types.TIME:
                setTime(parameterIndex, castToTime(x, targetSqlType));
                return;
            case Types.TIMESTAMP:
                setTimestamp(parameterIndex, castToTimestamp(x, targetSqlType));
                return;
            // TODO Types.TIME_WITH_TIMEZONE
            // TODO Types.TIMESTAMP_WITH_TIMEZONE
        }
        throw new SQLException("Unsupported target SQL type: " + targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType)
            throws SQLException
    {
        setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber());
    }

    @Override
    public void setObject(int parameterIndex, Object x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        }
        else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        }
        else if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
        }
        else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        }
        else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        }
        else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        }
        else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        }
        else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        }
        else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        }
        else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        }
        else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        }
        else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        }
        else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        }
        else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        }
        else {
            throw new SQLException("Unsupported object type: " + x.getClass().getName());
        }
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
        setNull(parameterIndex, sqlType);
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
        setString(parameterIndex, value);
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
    public ResultSet executeQuery(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public long executeLargeUpdate(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public boolean execute(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public boolean execute(String sql, String[] columnNames)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public void addBatch(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    private void setParameter(int parameterIndex, String value)
            throws SQLException
    {
        if (parameterIndex < 1) {
            throw new SQLException("Parameter index out of bounds: " + parameterIndex);
        }
        parameters.put(parameterIndex - 1, value);
    }

    private void formatParametersTo(StringBuilder builder)
            throws SQLException
    {
        List<String> values = new ArrayList<>();
        for (int index = 0; index < parameters.size(); index++) {
            if (!parameters.containsKey(index)) {
                throw new SQLException("No value specified for parameter " + (index + 1));
            }
            values.add(parameters.get(index));
        }
        Joiner.on(", ").appendTo(builder, values);
    }

    private String getExecuteSql()
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();
        sql.append("EXECUTE ").append(statementName);
        if (!parameters.isEmpty()) {
            sql.append(" USING ");
            formatParametersTo(sql);
        }
        return sql.toString();
    }

    private static String formatLiteral(String type, String x)
    {
        return type + " " + formatStringLiteral(x);
    }

    private static String formatBooleanLiteral(boolean x)
    {
        return Boolean.toString(x);
    }

    private static String formatStringLiteral(String x)
    {
        return "'" + x.replace("'", "''") + "'";
    }

    private static String formatBinaryLiteral(byte[] x)
    {
        return "X'" + base16().encode(x) + "'";
    }

    private static String typedNull(int targetSqlType)
            throws SQLException
    {
        switch (targetSqlType) {
            case Types.BOOLEAN:
            case Types.BIT:
                return typedNull("BOOLEAN");
            case Types.TINYINT:
                return typedNull("TINYINT");
            case Types.SMALLINT:
                return typedNull("SMALLINT");
            case Types.INTEGER:
                return typedNull("INTEGER");
            case Types.BIGINT:
                return typedNull("BIGINT");
            case Types.FLOAT:
            case Types.REAL:
                return typedNull("REAL");
            case Types.DOUBLE:
                return typedNull("DOUBLE");
            case Types.DECIMAL:
            case Types.NUMERIC:
                return typedNull("DECIMAL");
            case Types.CHAR:
            case Types.NCHAR:
                return typedNull("CHAR");
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return typedNull("VARCHAR");
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return typedNull("VARBINARY");
            case Types.DATE:
                return typedNull("DATE");
            case Types.TIME:
                return typedNull("TIME");
            case Types.TIMESTAMP:
                return typedNull("TIMESTAMP");
            // TODO Types.TIME_WITH_TIMEZONE
            // TODO Types.TIMESTAMP_WITH_TIMEZONE
            case Types.NULL:
                return "NULL";
        }
        throw new SQLException("Unsupported target SQL type: " + targetSqlType);
    }

    private static String typedNull(String prestoType)
    {
        return format("CAST(NULL AS %s)", prestoType);
    }
}
