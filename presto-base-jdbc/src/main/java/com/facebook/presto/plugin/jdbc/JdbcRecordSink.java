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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.joda.time.DateTimeZone.UTC;

public class JdbcRecordSink
        implements RecordSink
{
    private final Connection connection;
    private final PreparedStatement statement;

    private final int fieldCount;
    private final List<Type> columnTypes;
    private int field = -1;
    private int batchSize;

    public JdbcRecordSink(JdbcOutputTableHandle handle, JdbcClient jdbcClient)
    {
        try {
            connection = jdbcClient.getConnection(handle);
            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        try {
            statement = connection.prepareStatement(jdbcClient.buildInsertSql(handle));
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        fieldCount = handle.getColumnNames().size();
        columnTypes = handle.getColumnTypes();
    }

    @Override
    public void beginRecord()
    {
        checkState(field == -1, "already in record");
        field = 0;
    }

    @Override
    public void finishRecord()
    {
        checkState(field != -1, "not in record");
        checkState(field == fieldCount, "not all fields set");
        field = -1;

        try {
            statement.addBatch();
            batchSize++;

            if (batchSize >= 1000) {
                statement.executeBatch();
                connection.commit();
                connection.setAutoCommit(false);
                batchSize = 0;
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void appendNull()
    {
        try {
            statement.setObject(next(), null);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void appendBoolean(boolean value)
    {
        try {
            Type type = columnTypes.get(field);
            if (BOOLEAN.equals(type)) {
                statement.setBoolean(next(), value);
            }
            else {
                throw unsupportedType(type);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void appendLong(long value)
    {
        try {
            Type type = columnTypes.get(field);
            if (DATE.equals(type)) {
                // convert to midnight in default time zone
                long utcMillis = TimeUnit.DAYS.toMillis(value);
                long localMillis = UTC.getMillisKeepLocal(DateTimeZone.getDefault(), utcMillis);
                statement.setDate(next(), new Date(localMillis));
            }
            else if (TIMESTAMP.equals(type)) {
                // convert to default time zone
                long localMillis = UTC.getMillisKeepLocal(DateTimeZone.getDefault(), value);
                statement.setTimestamp(next(), new Timestamp(localMillis));
            }
            else if (TIME.equals(type)) {
                // convert to default time zone
                long localMillis = UTC.getMillisKeepLocal(DateTimeZone.getDefault(), value);
                statement.setTime(next(), new Time(localMillis));
            }
            else if (REAL.equals(type)) {
                statement.setFloat(next(), intBitsToFloat(toIntExact(value)));
            }
            else if (BIGINT.equals(type)) {
                statement.setLong(next(), value);
            }
            else if (INTEGER.equals(type)) {
                statement.setInt(next(), toIntExact(value));
            }
            else if (SMALLINT.equals(type)) {
                statement.setShort(next(), Shorts.checkedCast(value));
            }
            else if (TINYINT.equals(type)) {
                statement.setByte(next(), SignedBytes.checkedCast(value));
            }
            else {
                throw unsupportedType(type);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void appendDouble(double value)
    {
        try {
            Type type = columnTypes.get(field);
            if (DOUBLE.equals(type)) {
                statement.setDouble(next(), value);
            }
            else {
                throw unsupportedType(type);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void appendString(byte[] value)
    {
        try {
            Type type = columnTypes.get(field);
            if (isVarcharType(type) || isCharType(type)) {
                statement.setString(next(), new String(value, UTF_8));
            }
            else if (VARBINARY.equals(type)) {
                statement.setBytes(next(), value);
            }
            else {
                throw unsupportedType(type);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void appendObject(Object value)
    {
        throw unsupportedType(columnTypes.get(field));
    }

    @Override
    public Collection<Slice> commit()
    {
        // commit and close
        try (Connection connection = this.connection) {
            if (batchSize > 0) {
                statement.executeBatch();
                connection.commit();
            }
        }
        catch (SQLNonTransientException e) {
            throw new PrestoException(JDBC_NON_TRANSIENT_ERROR, e);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        // the committer does not need any additional info
        return ImmutableList.of();
    }

    @SuppressWarnings("UnusedDeclaration")
    @Override
    public void rollback()
    {
        // rollback and close
        try (Connection connection = this.connection;
                PreparedStatement statement = this.statement) {
            connection.rollback();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    private int next()
    {
        checkState(field != -1, "not in record");
        checkState(field < fieldCount, "all fields already set");
        field++;
        return field;
    }

    private static PrestoException unsupportedType(Type type)
    {
        return new PrestoException(NOT_SUPPORTED, "Type not supported for writing: " + type.getDisplayName());
    }
}
