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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KafkaMetadata
        implements ConnectorMetadata
{
    private final String connectorId;

    private final KafkaMetadataClient kafkaClient;

    @Inject
    public KafkaMetadata(KafkaConnectorId connectorId,
            KafkaMetadataClient kafkaMetadataClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.kafkaClient = requireNonNull(kafkaMetadataClient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(kafkaClient.getSchemaNames());
    }

    @Override
    @Nullable
    public KafkaTableHandle getTableHandle(ConnectorSession session,
            SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        KafkaTable table =
                kafkaClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new KafkaTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        KafkaTableHandle tableHandle = (KafkaTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new KafkaTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(
                new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session,
            ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
            ConnectorTableHandle table)
    {
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) table;
        checkArgument(kafkaTableHandle.getConnectorId().equals(connectorId),
                "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(
                kafkaTableHandle.getSchemaName(),
                kafkaTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session,
            String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = kafkaClient.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : kafkaClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) tableHandle;
        checkArgument(kafkaTableHandle.getConnectorId().equals(connectorId),
                "tableHandle is not for this connector");

        KafkaTable table = kafkaClient.getTable(kafkaTableHandle.getSchemaName(),
                kafkaTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(kafkaTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            KafkaColumnMetadata restColumnMetadata = (KafkaColumnMetadata) column;
            columnHandles.put(column.getName(),

                    new KafkaColumnHandle(
                            connectorId,
                            restColumnMetadata.getName(),
                            restColumnMetadata.getType(),
                            restColumnMetadata.getJsonPath(),
                            index));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns =
                ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Nullable
    public ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        KafkaTable table =
                kafkaClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(
                new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
            ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((KafkaColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        KafkaTableHandle restTableHandle = (KafkaTableHandle) tableHandle;
        KafkaTable restTable = kafkaClient.getTable(restTableHandle.getSchemaName(),
                restTableHandle.getTableName());
        if (restTable.containsIndexableColumns(indexableColumns)) {
            return Optional.of(new ConnectorResolvedIndex(
                    new KafkaIndexHandle(
                            new SchemaTableName(
                                    restTableHandle.getSchemaName(),
                                    restTableHandle.getTableName()),
                            tupleDomain),
                    tupleDomain));
        }
        else {
            return Optional.empty();
        }
    }
}
