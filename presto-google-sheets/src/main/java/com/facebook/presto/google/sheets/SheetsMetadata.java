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
package com.facebook.presto.google.sheets;

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
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class SheetsMetadata
        implements ConnectorMetadata
{
    private final SheetsClient sheetsClient;
    private static final List<String> SCHEMAS = ImmutableList.of("default");

    @Inject
    public SheetsMetadata(SheetsClient sheetsClient)
    {
        this.sheetsClient = requireNonNull(sheetsClient, "sheetsClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return SCHEMAS;
    }

    @Override
    public SheetsTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }
        try {
            Optional<SheetsTable> table = sheetsClient.getTable(tableName.getTableName());
            if (!table.isPresent()) {
                return null;
            }
            return new SheetsTableHandle(tableName.getSchemaName(), tableName.getTableName());
        }
        catch (PrestoException e) {
            return null;
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session, ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        SheetsTableHandle tableHandle = (SheetsTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new SheetsTableLayoutHandle(tableHandle));
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
        Optional<ConnectorTableMetadata> connectorTableMetadata = getTableMetadata(((SheetsTableHandle) table).toSchemaTableName());
        if (!connectorTableMetadata.isPresent()) {
            throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Metadata not found for table " + ((SheetsTableHandle) table).getTableName());
        }
        return connectorTableMetadata.get();
    }

    private Optional<ConnectorTableMetadata> getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return Optional.empty();
        }
        Optional<SheetsTable> table = sheetsClient.getTable(tableName.getTableName());
        if (table.isPresent()) {
            return Optional.of(new ConnectorTableMetadata(tableName, table.get().getColumnsMetadata()));
        }
        return Optional.empty();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SheetsTableHandle sheetsTableHandle = (SheetsTableHandle) tableHandle;
        Optional<SheetsTable> table = sheetsClient.getTable(sheetsTableHandle.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(sheetsTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.get().getColumnsMetadata()) {
            columnHandles.put(column.getName(), new SheetsColumnHandle(column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((SheetsColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, Optional.of(prefix.getSchemaName()))) {
            Optional<ConnectorTableMetadata> tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata.isPresent()) {
                columns.put(tableName, tableMetadata.get().getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        String schema = schemaName.orElse(getOnlyElement(SCHEMAS));

        if (listSchemaNames().contains(schema)) {
            return sheetsClient.getTableNames().stream()
                    .map(tableName -> new SchemaTableName(schema, tableName))
                    .collect(toImmutableList());
        }
        return ImmutableList.of();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SheetsTableHandle sheetsTableHandle = (SheetsTableHandle) tableHandle;
        return new SheetsInsertTableHandle(sheetsTableHandle.getSchemaName(),
                sheetsTableHandle.getTableName(), buildColumnHandles(sheetsTableHandle));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        List<SheetsColumnHandle> columns = buildColumnHandles(tableMetadata);
        sheetsClient.createSheet(tableMetadata.getTable().getTableName(), columns);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        List<SheetsColumnHandle> columns = buildColumnHandles(tableMetadata);
        sheetsClient.createSheet(tableMetadata.getTable().getTableName(), columns);
        return new SheetsOutputTableHandle(tableMetadata.getTable(), columns);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SheetsTableHandle table = (SheetsTableHandle) tableHandle;
        sheetsClient.dropSheet(table.getTableName());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        SheetsTableHandle handle = (SheetsTableHandle) tableHandle;
        sheetsClient.addColumn(handle.getTableName(), Arrays.asList(Arrays.asList(column.getName())), column.getType());
    }

    private static List<SheetsColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        AtomicInteger counter = new AtomicInteger(0);
        return tableMetadata.getColumns().stream()
                .map(m -> new SheetsColumnHandle(m.getName(), m.getType(), counter.getAndIncrement()))
                .collect(toList());
    }

    private List<SheetsColumnHandle> buildColumnHandles(SheetsTableHandle sheetsTableHandle)
    {
        AtomicInteger counter = new AtomicInteger(0);
        Optional<SheetsTable> table = sheetsClient.getTable(sheetsTableHandle.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(sheetsTableHandle.toSchemaTableName());
        }
        return table.get().getColumnsMetadata().stream()
                .map(m -> new SheetsColumnHandle(m.getName(), m.getType(), counter.getAndIncrement()))
                .collect(toList());
    }
}
