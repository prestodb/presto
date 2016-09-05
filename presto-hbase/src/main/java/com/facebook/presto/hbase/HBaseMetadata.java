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
package com.facebook.presto.hbase;

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.hbase.HBaseHandleResolver.convertColumnHandle;
import static com.facebook.presto.hbase.HBaseHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

/**
 * Manages the HBase connector specific meta data information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link HBaseInternalFieldDescription} for a list
 * of per-topic additional columns.
 */
public class HBaseMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final boolean hideInternalColumns;
    private final Map<SchemaTableName, HBaseTableDescription> tableDescriptions;
    private final Set<HBaseInternalFieldDescription> internalFieldDescriptions;

    @Inject
    public HBaseMetadata(
            HBaseConnectorId connectorId,
            HBaseConnectorConfig hbaseConnectorConfig,
            Supplier<Map<SchemaTableName, HBaseTableDescription>> hbaseTableDescriptionSupplier,
            Set<HBaseInternalFieldDescription> internalFieldDescriptions)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();

        requireNonNull(hbaseConnectorConfig, "hbaseConfig is null");
        this.hideInternalColumns = hbaseConnectorConfig.isHideInternalColumns();

        requireNonNull(hbaseTableDescriptionSupplier, "hbaseTableDescriptionSupplier is null");
        this.tableDescriptions = hbaseTableDescriptionSupplier.get();
        this.internalFieldDescriptions = requireNonNull(internalFieldDescriptions, "internalFieldDescriptions is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            builder.add(tableName.getSchemaName());
        }
        return ImmutableList.copyOf(builder.build());
    }

    @Override
    public HBaseTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        HBaseTableDescription table = tableDescriptions.get(schemaTableName);
        if (table == null) {
            return null;
        }

        return new HBaseTableHandle(connectorId,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getTopicName(),
                getDataFormat(table.getKey()),
                getDataFormat(table.getMessage()));
    }

    private static String getDataFormat(HBaseTableFieldGroup fieldGroup)
    {
        return (fieldGroup == null) ? DummyRowDecoder.NAME : fieldGroup.getDataFormat();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            if (schemaNameOrNull == null || tableName.getSchemaName().equals(schemaNameOrNull)) {
                builder.add(tableName);
            }
        }

        return builder.build();
    }

    //@SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HBaseTableHandle kafkaTableHandle = convertTableHandle(tableHandle);

        HBaseTableDescription hbaseTableDescription = tableDescriptions.get(kafkaTableHandle.toSchemaTableName());
        if (hbaseTableDescription == null) {
            throw new TableNotFoundException(kafkaTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        int index = 0;
        HBaseTableFieldGroup key = hbaseTableDescription.getKey();
        if (key != null) {
            List<HBaseTableFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (HBaseTableFieldDescription hbaseTopicFieldDescription : fields) {
                    columnHandles.put(hbaseTopicFieldDescription.getName(), hbaseTopicFieldDescription.getColumnHandle(connectorId, true, index++));
                }
            }
        }

        HBaseTableFieldGroup message = hbaseTableDescription.getMessage();
        if (message != null) {
            List<HBaseTableFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (HBaseTableFieldDescription hbaseTopicFieldDescription : fields) {
                    columnHandles.put(hbaseTopicFieldDescription.getName(), hbaseTopicFieldDescription.getColumnHandle(connectorId, false, index++));
                }
            }
        }

        for (HBaseInternalFieldDescription hbaseInternalFieldDescription : internalFieldDescriptions) {
            columnHandles.put(hbaseInternalFieldDescription.getName(), hbaseInternalFieldDescription.getColumnHandle(connectorId, index++, hideInternalColumns));
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames = prefix.getSchemaName() == null ? listTables(session, null) : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        HBaseTableHandle handle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new HBaseTableLayoutHandle(handle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    //@SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        HBaseTableDescription table = tableDescriptions.get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        HBaseTableFieldGroup key = table.getKey();
        if (key != null) {
            List<HBaseTableFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (HBaseTableFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        HBaseTableFieldGroup message = table.getMessage();
        if (message != null) {
            List<HBaseTableFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (HBaseTableFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        for (HBaseInternalFieldDescription fieldDescription : internalFieldDescriptions) {
            builder.add(fieldDescription.getColumnMetadata(hideInternalColumns));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }
}
