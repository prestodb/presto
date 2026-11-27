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
package com.facebook.presto.ducklake.catalog.jdbc;

import com.facebook.presto.ducklake.catalog.DuckLakeInlinedRowSource;
import com.facebook.presto.spi.PrestoException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_CATALOG_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * A {@link DuckLakeInlinedRowSource} backed by a JDBC {@link ResultSet}, owning the {@link
 * Connection} and {@link PreparedStatement} it was opened from. The result set's first column is
 * always {@code row_id}; the remaining columns are the caller-requested column list, so {@link
 * #getObject(int)} shifts its 0-based index by two to reach {@link ResultSet#getObject(int)}.
 */
class JdbcInlinedRowSource
        implements DuckLakeInlinedRowSource
{
    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;

    JdbcInlinedRowSource(Connection connection, PreparedStatement statement, ResultSet resultSet)
    {
        this.connection = requireNonNull(connection, "connection is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.resultSet = requireNonNull(resultSet, "resultSet is null");
    }

    @Override
    public boolean advanceNextRow()
    {
        try {
            return resultSet.next();
        }
        catch (SQLException e) {
            throw new PrestoException(DUCKLAKE_CATALOG_ERROR, "Failed to read DuckLake inlined table row: " + e.getMessage(), e);
        }
    }

    @Override
    public Object getObject(int columnIndex)
    {
        try {
            return resultSet.getObject(columnIndex + 2);
        }
        catch (SQLException e) {
            throw new PrestoException(DUCKLAKE_CATALOG_ERROR, "Failed to read DuckLake inlined table column: " + e.getMessage(), e);
        }
    }

    @Override
    public long getRowId()
    {
        try {
            return resultSet.getLong(1);
        }
        catch (SQLException e) {
            throw new PrestoException(DUCKLAKE_CATALOG_ERROR, "Failed to read DuckLake inlined table row id: " + e.getMessage(), e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        // Closed in reverse open order: resultSet, then statement, then connection.
        try (Connection ignoredConnection = connection;
                PreparedStatement ignoredStatement = statement;
                ResultSet ignoredResultSet = resultSet) {
            // no-op: try-with-resources performs the close
        }
        catch (SQLException e) {
            throw new IOException("Failed to close DuckLake inlined row source", e);
        }
    }
}
