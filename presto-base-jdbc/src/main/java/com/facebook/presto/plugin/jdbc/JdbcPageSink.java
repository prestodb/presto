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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.jdbc.mapping.WriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.BooleanWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.DoubleWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.LongWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.ObjectWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.SliceWriteFunction;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class JdbcPageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(JdbcPageSink.class);

    private final Connection connection;
    private final PreparedStatement statement;

    private final List<Type> columnTypes;
    private final List<WriteFunction> columnWriters;
    private int batchSize;

    public JdbcPageSink(ConnectorSession session, JdbcOutputTableHandle handle, JdbcClient jdbcClient)
    {
        try {
            connection = jdbcClient.getConnection(session, JdbcIdentity.from(session), handle);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        try {
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(jdbcClient.buildInsertSql(session, handle));
        }
        catch (SQLException e) {
            closeWithSuppression(connection, e);
            throw new PrestoException(JDBC_ERROR, e);
        }

        columnTypes = handle.getColumnTypes();
        columnWriters = columnTypes.stream().map(type -> {
            WriteFunction writeFunction = jdbcClient.toWriteMapping(type).getWriteFunction();
            verify(type.getJavaType() == writeFunction.getJavaType(),
                    format("Presto type %s is not compatible with write function %s accepting %s", type, writeFunction, writeFunction.getJavaType()));
            return writeFunction;
        }).collect(toImmutableList());
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

    private void appendColumn(Page page, int position, int channel)
            throws SQLException
    {
        Block block = page.getBlock(channel);
        int parameter = channel + 1;

        if (block.isNull(position)) {
            statement.setObject(parameter, null);
            return;
        }

        Type type = columnTypes.get(channel);
        Class<?> javaType = type.getJavaType();
        WriteFunction writeFunction = columnWriters.get(channel);
        if (javaType == boolean.class) {
            ((BooleanWriteFunction) writeFunction).set(statement, parameter, type.getBoolean(block, position));
        }
        else if (javaType == long.class) {
            ((LongWriteFunction) writeFunction).set(statement, parameter, type.getLong(block, position));
        }
        else if (javaType == double.class) {
            ((DoubleWriteFunction) writeFunction).set(statement, parameter, type.getDouble(block, position));
        }
        else if (javaType == Slice.class) {
            ((SliceWriteFunction) writeFunction).set(statement, parameter, type.getSlice(block, position));
        }
        else {
            try {
                ((ObjectWriteFunction) writeFunction).set(statement, parameter, type.getObject(block, position));
            }
            catch (SQLException e) {
                throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
            }
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
            // Exceptions happened during abort do not cause any real damage so ignore them
            log.debug(e, "SQLException when abort");
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
}
