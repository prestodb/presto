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
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

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
            statement.setBoolean(next(), value);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void appendLong(long value)
    {
        try {
            if (DATE.equals(columnTypes.get(field))) {
                // convert to midnight in default time zone
                long utcMillis = TimeUnit.DAYS.toMillis(value);
                long localMillis = ISOChronology.getInstanceUTC().getZone().getMillisKeepLocal(DateTimeZone.getDefault(), utcMillis);
                statement.setDate(next(), new Date(localMillis));
            }
            else {
                statement.setLong(next(), value);
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
            statement.setDouble(next(), value);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void appendString(byte[] value)
    {
        try {
            statement.setString(next(), new String(value, UTF_8));
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void appendObject(Object value)
    {
        throw new UnsupportedOperationException();
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
}
