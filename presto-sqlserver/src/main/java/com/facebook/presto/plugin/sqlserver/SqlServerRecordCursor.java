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
package com.facebook.presto.plugin.sqlserver;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class SqlServerRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(SqlServerRecordCursor.class);

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private final List<JdbcColumnHandle> columnHandles;
    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;

    private boolean closed;

    public SqlServerRecordCursor(JdbcClient jdbcClient, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));

        try {
            connection = requireNonNull(jdbcClient, "jdbcClient is null").getConnection(requireNonNull(split, "split was null"));
            statement = jdbcClient.buildSql(connection, split, columnHandles);
            log.debug("Executing: %s", statement.toString());
            resultSet = statement.executeQuery();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (closed) {
            return false;
        }

        try {
            boolean result = resultSet.next();
            if (!result) {
                close();
            }
            return result;
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return resultSet.getBoolean(field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            Type type = getType(field);
            if (TINYINT.equals(type)) {
                return resultSet.getByte(field + 1);
            }
            if (SMALLINT.equals(type)) {
                return resultSet.getShort(field + 1);
            }
            if (INTEGER.equals(type)) {
                return resultSet.getInt(field + 1);
            }
            if (BIGINT.equals(type)) {
                return resultSet.getLong(field + 1);
            }
            if (REAL.equals(type)) {
                return floatToRawIntBits(resultSet.getFloat(field + 1));
            }
            if (DATE.equals(type)) {
                Calendar cal = Calendar.getInstance(UTC);
                Date date = resultSet.getDate(field + 1, cal);
                return TimeUnit.MILLISECONDS.toDays(date.getTime());
            }
            if (TIME.equals(type)) {
                Calendar cal = Calendar.getInstance(UTC);
                Time time = resultSet.getTime(field + 1, cal);
                return time.getTime();
            }
            if (TIMESTAMP.equals(type)) {
                Calendar cal = Calendar.getInstance(UTC);
                Timestamp ts = resultSet.getTimestamp(field + 1, cal);
                return ts.getTime();
            }
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for long: " + type.getTypeSignature());
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return resultSet.getDouble(field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            Type type = getType(field);
            if (type instanceof VarcharType) {
                return utf8Slice(resultSet.getString(field + 1));
            }
            if (type instanceof CharType) {
                return utf8Slice(CharMatcher.is(' ').trimTrailingFrom(resultSet.getString(field + 1)));
            }
            if (VARBINARY.equals(type)) {
                return wrappedBuffer(resultSet.getBytes(field + 1));
            }
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for slice: " + type.getTypeSignature());
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.size(), "Invalid field index");

        try {
            // JDBC is kind of dumb: we need to read the field and then ask
            // if it was null, which means we are wasting effort here.
            // We could save the result of the field access if it matters.
            resultSet.getObject(field + 1);

            return resultSet.wasNull();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @SuppressWarnings({"UnusedDeclaration", "EmptyTryBlock"})
    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        // use try with resources to close everything properly
        try (Connection connection = this.connection;
                Statement statement = this.statement;
                ResultSet resultSet = this.resultSet) {
            // do nothing
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private RuntimeException handleSqlException(Exception e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
        return new RuntimeException(e);
    }
}
