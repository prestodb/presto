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

package com.facebook.presto.plugin.blackhole;

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
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.DISTRIBUTED_ON;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.FIELD_LENGTH_PROPERTY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.PAGES_PER_SPLIT_PROPERTY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.PAGE_PROCESSING_DELAY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.ROWS_PER_PAGE_PROPERTY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.SPLIT_COUNT_PROPERTY;
import static com.facebook.presto.plugin.blackhole.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static java.text.MessageFormat.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class BlackHoleMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final Map<String, BlackHoleTableHandle> tables = new ConcurrentHashMap<>();

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return tables.get(tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle blackHoleTableHandle = checkType(tableHandle, BlackHoleTableHandle.class, "tableHandle");
        return blackHoleTableHandle.toTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull != null && !schemaNameOrNull.equals(SCHEMA_NAME)) {
            return ImmutableList.of();
        }
        return tables.values().stream()
                .map(BlackHoleTableHandle::toSchemaTableName)
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle blackHoleTableHandle = checkType(tableHandle, BlackHoleTableHandle.class, "tableHandle");
        return blackHoleTableHandle.getColumnHandles().stream()
                .collect(toMap(BlackHoleColumnHandle::getName, column -> column));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        BlackHoleColumnHandle blackHoleColumnHandle = checkType(columnHandle, BlackHoleColumnHandle.class, "columnHandle");
        return blackHoleColumnHandle.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.toSchemaTableName()))
                .collect(toMap(BlackHoleTableHandle::toSchemaTableName, handle -> handle.toTableMetadata().getColumns()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle blackHoleTableHandle = checkType(tableHandle, BlackHoleTableHandle.class, "tableHandle");
        tables.remove(blackHoleTableHandle.getTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        BlackHoleTableHandle oldTableHandle = checkType(tableHandle, BlackHoleTableHandle.class, "tableHandle");
        BlackHoleTableHandle newTableHandle = new BlackHoleTableHandle(
                oldTableHandle.getSchemaName(),
                newTableName.getTableName(),
                oldTableHandle.getColumnHandles(),
                oldTableHandle.getSplitCount(),
                oldTableHandle.getPagesPerSplit(),
                oldTableHandle.getRowsPerPage(),
                oldTableHandle.getFieldsLength(),
                oldTableHandle.getPageProcessingDelay());
        tables.remove(oldTableHandle.getTableName());
        tables.put(newTableName.getTableName(), newTableHandle);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of());
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession connectorSession, ConnectorTableMetadata tableMetadata)
    {
        List<String> distributeColumns = (List<String>) tableMetadata.getProperties().get(DISTRIBUTED_ON);
        if (distributeColumns.isEmpty()) {
            return Optional.empty();
        }

        Set<String> undefinedColumns = Sets.difference(
                ImmutableSet.copyOf(distributeColumns),
                tableMetadata.getColumns().stream()
                        .map(ColumnMetadata::getName)
                        .collect(toSet()));
        if (!undefinedColumns.isEmpty()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Distribute columns not defined on table: " + undefinedColumns);
        }

        return Optional.of(new ConnectorNewTableLayout(BlackHolePartitioningHandle.INSTANCE, distributeColumns));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        int splitCount = (Integer) tableMetadata.getProperties().get(SPLIT_COUNT_PROPERTY);
        int pagesPerSplit = (Integer) tableMetadata.getProperties().get(PAGES_PER_SPLIT_PROPERTY);
        int rowsPerPage = (Integer) tableMetadata.getProperties().get(ROWS_PER_PAGE_PROPERTY);
        int fieldsLength = (Integer) tableMetadata.getProperties().get(FIELD_LENGTH_PROPERTY);

        if (splitCount < 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, SPLIT_COUNT_PROPERTY + " property is negative");
        }
        if (pagesPerSplit < 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, PAGES_PER_SPLIT_PROPERTY + " property is negative");
        }
        if (rowsPerPage < 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, ROWS_PER_PAGE_PROPERTY + " property is negative");
        }

        if (((splitCount > 0) || (pagesPerSplit > 0) || (rowsPerPage > 0)) &&
                ((splitCount == 0) || (pagesPerSplit == 0) || (rowsPerPage == 0))) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("All properties [%s, %s, %s] must be set if any are set",
                    SPLIT_COUNT_PROPERTY, PAGES_PER_SPLIT_PROPERTY, ROWS_PER_PAGE_PROPERTY));
        }

        Duration pageProcessingDelay = (Duration) tableMetadata.getProperties().get(PAGE_PROCESSING_DELAY);

        BlackHoleTableHandle handle = new BlackHoleTableHandle(
                tableMetadata,
                splitCount,
                pagesPerSplit,
                rowsPerPage,
                fieldsLength,
                pageProcessingDelay);
        return new BlackHoleOutputTableHandle(handle, pageProcessingDelay);
    }

    @Override
    public void finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        BlackHoleOutputTableHandle blackHoleOutputTableHandle = checkType(tableHandle, BlackHoleOutputTableHandle.class, "tableHandle");
        BlackHoleTableHandle table = blackHoleOutputTableHandle.getTable();
        tables.put(table.getTableName(), table);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle handle = checkType(tableHandle, BlackHoleTableHandle.class, "tableHandle");
        return new BlackHoleInsertTableHandle(handle.getPageProcessingDelay());
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
        BlackHoleTableHandle blackHoleHandle = checkType(handle, BlackHoleTableHandle.class, "handle");
        BlackHoleTableLayoutHandle layoutHandle = new BlackHoleTableLayoutHandle(
                blackHoleHandle.getSplitCount(),
                blackHoleHandle.getPagesPerSplit(),
                blackHoleHandle.getRowsPerPage(),
                blackHoleHandle.getFieldsLength(),
                blackHoleHandle.getPageProcessingDelay());
        return ImmutableList.of(new ConnectorTableLayoutResult(getTableLayout(session, layoutHandle), TupleDomain.all()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(
                handle,
                Optional.empty(),
                TupleDomain.none(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }
}
