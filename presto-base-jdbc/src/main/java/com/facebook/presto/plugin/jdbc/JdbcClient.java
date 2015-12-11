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

import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface JdbcClient
{
    Set<String> getSchemaNames();

    List<SchemaTableName> getTableNames(@Nullable String schema);

    @Nullable
    JdbcTableHandle getTableHandle(SchemaTableName schemaTableName);

    List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle);

    ConnectorSplitSource getSplits(JdbcTableLayoutHandle layoutHandle);

    Connection getConnection(JdbcSplit split)
            throws SQLException;

    PreparedStatement buildSql(JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException;

    JdbcOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata);

    void commitCreateTable(JdbcOutputTableHandle handle, Collection<Slice> fragments);

    void dropTable(JdbcTableHandle jdbcTableHandle);

    void rollbackCreateTable(JdbcOutputTableHandle handle);

    String buildInsertSql(JdbcOutputTableHandle handle);

    Connection getConnection(JdbcOutputTableHandle handle)
            throws SQLException;

    PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException;
}
