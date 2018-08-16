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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchOutputTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ElasticsearchMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final BaseClient client;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();  //lock

    ElasticsearchMetadata(
            ElasticsearchConnectorId connectorId,
            BaseClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(client.getSchemaNames());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        // Need to validate that SchemaTableName is a table
        if (!client.existsTable(tableName)) {
            return null;
        }
        return new ElasticsearchTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        ElasticsearchTableHandle tableHandle = (ElasticsearchTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new ElasticsearchTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        checkArgument(handle.getConnectorId().equals(connectorId), "table is not for this connector");
        SchemaTableName tableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
        ConnectorTableMetadata metadata = getTableMetadata(tableName);
        if (metadata == null) {
            throw new TableNotFoundException(tableName);
        }
        return metadata;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = client.getSchemaNames();
        }
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : client.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) tableHandle;
        checkArgument(handle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        ElasticsearchTable table = client.getTable(handle.getSchemaTableName());
        if (table == null) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }
        return table.getColumns().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ElasticsearchColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        // Need to validate that SchemaTableName is a table
        ElasticsearchTable table = client.getTable(tableName);
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkNoRollback();
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) tableHandle;
        setRollback(() -> {
            // Rollbacks for inserts are off the table when it comes to data in Hbase.
            // When a batch of Mutations fails to be inserted, the general strategy
            // is to run the insert operation again until it is successful
            // Any mutations that were successfully written will be overwritten
            // with the same values, so that isn't a problem.
            throw new PrestoException(NOT_SUPPORTED, format("Unable to rollback insert for table %s.%s. Some rows may have been written. Please run your insert again.", handle.getSchemaName(), handle.getTableName()));
        });
        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        clearRollback();
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) tableHandle;
        if (handle != null) {
            client.dropTable(handle.getSchemaTableName());
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        client.createTable(tableMetadata);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        checkNoRollback();

        SchemaTableName tableName = tableMetadata.getTable();
        client.createTable(tableMetadata);

        //--- es column is sort ----
        List<ElasticsearchColumnHandle> columns = tableMetadata.getColumns().stream()
                .map(columnMetadata -> new ElasticsearchColumnHandle(
                        columnMetadata.getName(),
                        columnMetadata.getType(),
                        columnMetadata.getComment(),
                        true,
                        columnMetadata.isHidden())).collect(Collectors.toList());
        ElasticsearchOutputTableHandle handle = new ElasticsearchOutputTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                columns);

        setRollback(() -> client.dropTable(tableMetadata.getTable()));  //Rollback operation

        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        clearRollback();
        return Optional.empty();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        // List all tables if schema or table is null
        if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
            return listTables(session, prefix.getSchemaName());
        }

        // Make sure requested table exists, returning the single table of it does
        SchemaTableName table = new SchemaTableName(prefix.getSchemaName(), prefix.getTableName());
        if (getTableHandle(session, table) != null) {
            return ImmutableList.of(table);
        }

        // Else, return empty list
        return ImmutableList.of();
    }

    private void checkNoRollback()
    {
        checkState(rollbackAction.get() == null, "Cannot begin a new write while in an existing one");
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "Should not have to override existing rollback action");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    void rollback()
    {
        Runnable rollbackAction = this.rollbackAction.getAndSet(null);
        if (rollbackAction != null) {
            rollbackAction.run();
        }
    }
}
