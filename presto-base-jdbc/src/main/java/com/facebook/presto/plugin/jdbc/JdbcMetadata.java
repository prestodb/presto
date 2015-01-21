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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.plugin.jdbc.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcMetadata
        implements ConnectorMetadata
{
    private final JdbcClient jdbcClient;

    @Inject
    public JdbcMetadata(JdbcConnectorId connectorId, JdbcClient jdbcClient)
    {
        this.jdbcClient = checkNotNull(jdbcClient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(jdbcClient.getSchemaNames());
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return jdbcClient.getTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table)
    {
        JdbcTableHandle handle = checkType(table, JdbcTableHandle.class, "tableHandle");

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (JdbcColumnHandle column : jdbcClient.getColumns(handle)) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return jdbcClient.getTableNames(schemaNameOrNull);
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle jdbcTableHandle = checkType(tableHandle, JdbcTableHandle.class, "tableHandle");

        ImmutableMap.Builder<String, ConnectorColumnHandle> columnHandles = ImmutableMap.builder();
        for (JdbcColumnHandle column : jdbcClient.getColumns(jdbcTableHandle)) {
            columnHandles.put(column.getColumnMetadata().getName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchemaName())) {
            try {
                JdbcTableHandle tableHandle = jdbcClient.getTableHandle(tableName);
                if (tableHandle == null) {
                    continue;
                }
                columns.put(tableName, getTableMetadata(tableHandle).getColumns());
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        checkType(tableHandle, JdbcTableHandle.class, "tableHandle");
        return checkType(columnHandle, JdbcColumnHandle.class, "columnHandle").getColumnMetadata();
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return false;
    }

    @Override
    public ConnectorTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return jdbcClient.beginCreateTable(tableMetadata);
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        JdbcOutputTableHandle handle = checkType(tableHandle, JdbcOutputTableHandle.class, "tableHandle");
        jdbcClient.commitCreateTable(handle, fragments);
    }

    @Override
    public void renameTable(ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return ImmutableList.of();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return ImmutableMap.of();
    }
}
