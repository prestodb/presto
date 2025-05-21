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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.TableStatistics;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface JdbcClient
{
    default boolean schemaExists(ConnectorSession session, JdbcIdentity identity, String schema)
    {
        return getSchemaNames(session, identity).contains(schema);
    }

    String getIdentifierQuote();

    Set<String> getSchemaNames(ConnectorSession session, JdbcIdentity identity);

    List<SchemaTableName> getTableNames(ConnectorSession session, JdbcIdentity identity, Optional<String> schema);

    @Nullable
    JdbcTableHandle getTableHandle(ConnectorSession session, JdbcIdentity identity, SchemaTableName schemaTableName);

    List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle);

    Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle);

    ConnectorSplitSource getSplits(ConnectorSession session, JdbcIdentity identity, JdbcTableLayoutHandle layoutHandle);

    Connection getConnection(ConnectorSession session, JdbcIdentity identity, JdbcSplit split)
            throws SQLException;

    default void abortReadConnection(Connection connection)
            throws SQLException
    {
        // most drivers do not need this
    }

    PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException;

    void addColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, ColumnMetadata column);

    void dropColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column);

    void renameColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName);

    void renameTable(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName);

    void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    void commitCreateTable(ConnectorSession session, JdbcIdentity identity, JdbcOutputTableHandle handle);

    JdbcOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    void finishInsertTable(ConnectorSession session, JdbcIdentity identity, JdbcOutputTableHandle handle);

    void dropTable(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle jdbcTableHandle);

    void truncateTable(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle jdbcTableHandle);

    void rollbackCreateTable(ConnectorSession session, JdbcIdentity identity, JdbcOutputTableHandle handle);

    String buildInsertSql(ConnectorSession session, JdbcOutputTableHandle handle);

    Connection getConnection(ConnectorSession session, JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException;

    PreparedStatement getPreparedStatement(ConnectorSession session, Connection connection, String sql)
            throws SQLException;

    TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, List<JdbcColumnHandle> columnHandles, TupleDomain<ColumnHandle> tupleDomain);

    String normalizeIdentifier(ConnectorSession session, String identifier);
}
