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

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.readBigDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;

public class JdbcPageSink
        implements ConnectorPageSink
{
    private final Connection connection;
    private final PreparedStatement statement;

    private final List<Type> columnTypes;
    private int batchSize;

    public JdbcPageSink(JdbcOutputTableHandle handle, JdbcClient jdbcClient)
    {
        try {
            connection = jdbcClient.getConnection(handle);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        try {
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(jdbcClient.buildInsertSql(handle));
        }
        catch (SQLException e) {
            closeWithSuppression(connection, e);
            throw new PrestoException(JDBC_ERROR, e);
        }

        columnTypes = handle.getColumnTypes();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    appendColumn(page, position, channel);
                }

                statement.addBatch();
                batchSize++;

                if (batchSize >= 1000) {
                    statement.executeBatch();
                    connection.commit();
                    connection.setAutoCommit(false);
                    batchSize = 0;
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return NOT_BLOCKED;
    }

    public void appendColumn(Page page, int position, int channel)
            throws SQLException
    {
        Block block = page.getBlock(channel);
        int parameter = channel + 1;
        Type type = columnTypes.get(channel);

        if (block.isNull(position)) {
            setNull(type, parameter, block, position);
        }
        else if (BOOLEAN.equals(type)) {
            setBoolean(type, parameter, block, position);
        }
        else if (BIGINT.equals(type)) {
            setBigint(type, parameter, block, position);
        }
        else if (INTEGER.equals(type)) {
            setInteger(type, parameter, block, position);
        }
        else if (SMALLINT.equals(type)) {
            setSmallint(type, parameter, block, position);
        }
        else if (TINYINT.equals(type)) {
            setTinyint(type, parameter, block, position);
        }
        else if (DOUBLE.equals(type)) {
            setDouble(type, parameter, block, position);
        }
        else if (REAL.equals(type)) {
            setReal(type, parameter, block, position);
        }
        else if (type instanceof DecimalType) {
            setDecimal(type, parameter, block, position);
        }
        else if (isVarcharType(type)) {
            setVarchar(type, parameter, block, position);
        }
        else if (isCharType(type)) {
            setChar(type, parameter, block, position);
        }
        else if (VARBINARY.equals(type)) {
            setVarbinary(type, parameter, block, position);
        }
        else if (DATE.equals(type)) {
            setDate(type, parameter, block, position);
        }
        else if (TIME.equals(type)) {
            setTime(type, parameter, block, position);
        }
        else if (TIME_WITH_TIME_ZONE.equals(type)) {
            setTimeWithTimeZone(type, parameter, block, position);
        }
        else if (TIMESTAMP.equals(type)) {
            setTimestamp(type, parameter, block, position);
        }
        else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            setTimestampWithTimeZone(type, parameter, block, position);
        }
        else if (isOtherType(type)) {
            setOtherType(type, parameter, block, position);
        }
        else {
            throw unsupportedType(type);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // commit and close
        try (Connection connection = this.connection;
                PreparedStatement statement = this.statement) {
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
        return completedFuture(ImmutableList.of());
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
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

    @SuppressWarnings("ObjectEquality")
    private static void closeWithSuppression(Connection connection, Throwable throwable)
    {
        try {
            connection.close();
        }
        catch (Throwable t) {
            // Self-suppression not permitted
            if (throwable != t) {
                throwable.addSuppressed(t);
            }
        }
    }

    protected PreparedStatement statement()
    {
        return statement;
    }

    protected final PrestoException unsupportedType(Type type)
    {
        return new PrestoException(NOT_SUPPORTED,
                "Unsupported column type: " + type.getDisplayName());
    }

    protected void setNull(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setObject(parameter, null);
    }

    protected void setBoolean(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setBoolean(parameter, BOOLEAN.getBoolean(block, position));
    }

    protected void setTinyint(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setByte(parameter, SignedBytes.checkedCast(TINYINT.getLong(block, position)));
    }

    protected void setSmallint(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setShort(parameter, Shorts.checkedCast(SMALLINT.getLong(block, position)));
    }

    protected void setInteger(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setInt(parameter, toIntExact(INTEGER.getLong(block, position)));
    }

    protected void setBigint(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setLong(parameter, BIGINT.getLong(block, position));
    }

    protected void setReal(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setFloat(parameter, intBitsToFloat(toIntExact(REAL.getLong(block, position))));
    }

    protected void setDouble(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setDouble(parameter, DOUBLE.getDouble(block, position));
    }

    protected void setDecimal(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setBigDecimal(parameter, readBigDecimal((DecimalType) type, block, position));
    }

    protected void setVarchar(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setString(parameter, type.getSlice(block, position).toStringUtf8());
    }

    protected void setChar(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setString(parameter, type.getSlice(block, position).toStringUtf8());
    }

    protected void setVarbinary(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        statement.setBytes(parameter, type.getSlice(block, position).getBytes());
    }

    protected void setDate(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        // convert to midnight in default time zone
        long utcMillis = DAYS.toMillis(type.getLong(block, position));
        long localMillis = getInstanceUTC().getZone()
                .getMillisKeepLocal(DateTimeZone.getDefault(), utcMillis);
        statement.setDate(parameter, new Date(localMillis));
    }

    protected void setTime(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        throw unsupportedType(type);
    }

    protected void setTimeWithTimeZone(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        throw unsupportedType(type);
    }

    protected void setTimestamp(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        throw unsupportedType(type);
    }

    protected void setTimestampWithTimeZone(Type type, int parameter, Block block, int position)
            throws SQLException
    {
        throw unsupportedType(type);
    }

    /**
     * @return Whether the given {@link Type} is a supported type that does not
     * have its own setter method.
     */
    protected boolean isOtherType(Type type)
    {
        return false;
    }

    /**
     * Set a column of a type that does not have its own setter method.
     */
    protected void setOtherType(Type type, int parameter, Block block, int position)
    {
        throw unsupportedType(type);
    }
}
