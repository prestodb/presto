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

package com.facebook.presto.plugin.memory;

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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.plugin.memory.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class MemoryMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final String connectorId;
    private final AtomicLong nextTableId = new AtomicLong();
    private final Map<String, Long> tableIds = new ConcurrentHashMap<>();
    private final Map<Long, MemoryTableHandle> tables = new ConcurrentHashMap<>();

    public MemoryMetadata(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Long tableId = tableIds.get(tableName.getTableName());
        if (tableId == null) {
            return null;
        }
        return tables.get(tableId).updateActiveTableIds(ImmutableSet.copyOf(tableIds.values()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle memoryTableHandle = checkType(tableHandle, MemoryTableHandle.class, "tableHandle");
        return memoryTableHandle.toTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull != null && !schemaNameOrNull.equals(SCHEMA_NAME)) {
            return ImmutableList.of();
        }
        return tables.values().stream()
                .map(MemoryTableHandle::toSchemaTableName)
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle memoryTableHandle = checkType(tableHandle, MemoryTableHandle.class, "tableHandle");
        return memoryTableHandle.getColumnHandles().stream()
                .collect(toMap(MemoryColumnHandle::getName, column -> column));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        MemoryColumnHandle memoryColumnHandle = checkType(columnHandle, MemoryColumnHandle.class, "columnHandle");
        return memoryColumnHandle.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.toSchemaTableName()))
                .collect(toMap(MemoryTableHandle::toSchemaTableName, handle -> handle.toTableMetadata().getColumns()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = checkType(tableHandle, MemoryTableHandle.class, "tableHandle");
        Long tableId = tableIds.remove(handle.getTableName());
        if (tableId != null) {
            tables.remove(tableId);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        MemoryTableHandle oldTableHandle = checkType(tableHandle, MemoryTableHandle.class, "tableHandle");
        MemoryTableHandle newTableHandle = new MemoryTableHandle(
                oldTableHandle.getConnectorId(),
                oldTableHandle.getSchemaName(),
                newTableName.getTableName(),
                oldTableHandle.getTableId(),
                oldTableHandle.getColumnHandles(),
                oldTableHandle.getActiveTableIds()
        );
        tableIds.remove(oldTableHandle.getTableName());
        tableIds.put(newTableName.getTableName(), oldTableHandle.getTableId());
        tables.remove(oldTableHandle.getTableId());
        tables.put(oldTableHandle.getTableId(), newTableHandle);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        long nextId = nextTableId.getAndIncrement();
        Set<Long> activeTableIds = new HashSet(tableIds.values());
        activeTableIds.add(nextId);
        return new MemoryOutputTableHandle(new MemoryTableHandle(
                connectorId,
                nextId,
                tableMetadata,
                activeTableIds));
    }

    @Override
    public void finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        MemoryOutputTableHandle memoryOutputTableHandle = checkType(tableHandle, MemoryOutputTableHandle.class, "tableHandle");
        MemoryTableHandle table = memoryOutputTableHandle.getTable();
        tableIds.put(table.getTableName(), table.getTableId());
        tables.put(table.getTableId(), table);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle memoryTableHandle = checkType(tableHandle, MemoryTableHandle.class, "tableHandle");
        return new MemoryInsertTableHandle(memoryTableHandle);
    }

    @Override
    public void finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments) {}

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        requireNonNull(handle, "handle is null");
        checkArgument(handle instanceof MemoryTableHandle);

        MemoryTableLayoutHandle layoutHandle = new MemoryTableLayoutHandle((MemoryTableHandle) handle);
        return ImmutableList.of(new ConnectorTableLayoutResult(getTableLayout(session, layoutHandle), constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(
                handle,
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }
}
