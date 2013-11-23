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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.InternalTable;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.tuple.TupleInfo.Type.fromColumnType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;

public class InformationSchemaDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private final Metadata metadata;
    private final SplitManager splitManager;

    @Inject
    public InformationSchemaDataStreamProvider(Metadata metadata, SplitManager splitManager)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof InformationSchemaSplit;
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
    {
        List<BlockIterable> channels = createChannels(split, columns);
        return new AlignmentOperator(operatorContext, channels);
    }

    private List<BlockIterable> createChannels(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof InformationSchemaSplit, "Split must be of type %s, not %s", InformationSchemaSplit.class.getName(), split.getClass().getName());

        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        InformationSchemaTableHandle handle = ((InformationSchemaSplit) split).getTableHandle();
        Map<String, Object> filters = ((InformationSchemaSplit) split).getFilters();

        InternalTable table = getInformationSchemaTable(handle.getCatalogName(), handle.getSchemaTableName(), filters);

        ImmutableList.Builder<BlockIterable> list = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            checkArgument(column instanceof InformationSchemaColumnHandle, "column must be of type %s, not %s", InformationSchemaColumnHandle.class.getName(), column.getClass().getName());
            InformationSchemaColumnHandle internalColumn = (InformationSchemaColumnHandle) column;

            list.add(table.getColumn(internalColumn.getColumnName()));
        }
        return list.build();
    }

    public InternalTable getInformationSchemaTable(String catalog, SchemaTableName table, Map<String, Object> filters)
    {
        if (table.equals(InformationSchemaMetadata.TABLE_COLUMNS)) {
            return buildColumns(catalog, filters);
        }
        else if (table.equals(InformationSchemaMetadata.TABLE_TABLES)) {
            return buildTables(catalog, filters);
        }
        else if (table.equals(InformationSchemaMetadata.TABLE_SCHEMATA)) {
            return buildSchemata(catalog);
        }
        else if (table.equals(InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS)) {
            return buildFunctions();
        }
        else if (table.equals(InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS)) {
            return buildPartitions(catalog, filters);
        }

        throw new IllegalArgumentException(format("table does not exist: %s", table));
    }

    private InternalTable buildColumns(String catalogName, Map<String, Object> filters)
    {
        InternalTable.Builder table = InternalTable.builder(InformationSchemaMetadata.informationSchemaTableColumns(InformationSchemaMetadata.TABLE_COLUMNS));
        for (Entry<QualifiedTableName, List<ColumnMetadata>> entry : getColumnsList(catalogName, filters).entrySet()) {
            QualifiedTableName tableName = entry.getKey();
            for (ColumnMetadata column : entry.getValue()) {
                table.add(table.getTupleInfo().builder()
                        .append(tableName.getCatalogName())
                        .append(tableName.getSchemaName())
                        .append(tableName.getTableName())
                        .append(column.getName())
                        .append(column.getOrdinalPosition() + 1)
                        .appendNull()
                        .append("YES")
                        .append(fromColumnType(column.getType()).getName())
                        .append(column.isPartitionKey() ? "YES" : "NO")
                        .build());
            }
        }
        return table.build();
    }

    private Map<QualifiedTableName, List<ColumnMetadata>> getColumnsList(String catalogName, Map<String, Object> filters)
    {
        return metadata.listTableColumns(extractQualifiedTablePrefix(catalogName, filters));
    }

    private InternalTable buildTables(String catalogName, Map<String, Object> filters)
    {
        InternalTable.Builder table = InternalTable.builder(InformationSchemaMetadata.informationSchemaTableColumns(InformationSchemaMetadata.TABLE_TABLES));
        for (QualifiedTableName name : getTablesList(catalogName, filters)) {
            table.add(table.getTupleInfo().builder()
                    .append(name.getCatalogName())
                    .append(name.getSchemaName())
                    .append(name.getTableName())
                    .append("BASE TABLE")
                    .build());
        }
        return table.build();
    }

    private List<QualifiedTableName> getTablesList(String catalogName, Map<String, Object> filters)
    {
        return metadata.listTables(extractQualifiedTablePrefix(catalogName, filters));
    }

    private InternalTable buildFunctions()
    {
        InternalTable.Builder table = InternalTable.builder(InformationSchemaMetadata.informationSchemaTableColumns(InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS));
        for (FunctionInfo function : metadata.listFunctions()) {
            Iterable<String> arguments = transform(function.getArgumentTypes(), Type.nameGetter());

            String functionType;
            if (function.isAggregate()) {
                functionType = "aggregate";
            }
            else if (function.isWindow()) {
                functionType = "window";
            }
            else if (function.isDeterministic()) {
                functionType = "scalar";
            }
            else {
                functionType = "scalar (non-deterministic)";
            }

            table.add(table.getTupleInfo().builder()
                    .append(function.getName().toString())
                    .append(Joiner.on(", ").join(arguments))
                    .append(function.getReturnType().getName())
                    .append(functionType)
                    .append(nullToEmpty(function.getDescription()))
                    .build());
        }
        return table.build();
    }

    private InternalTable buildSchemata(String catalogName)
    {
        InternalTable.Builder table = InternalTable.builder(InformationSchemaMetadata.informationSchemaTableColumns(InformationSchemaMetadata.TABLE_SCHEMATA));
        for (String schema : metadata.listSchemaNames(catalogName)) {
            table.add(table.getTupleInfo().builder()
                    .append(catalogName)
                    .append(schema)
                    .build());
        }
        return table.build();
    }

    private InternalTable buildPartitions(String catalogName, Map<String, Object> filters)
    {
        QualifiedTableName tableName = extractQualifiedTableName(catalogName, filters);

        InternalTable.Builder table = InternalTable.builder(InformationSchemaMetadata.informationSchemaTableColumns(InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS));
        int partitionNumber = 1;

        Optional<TableHandle> tableHandle = metadata.getTableHandle(tableName);
        checkArgument(tableHandle.isPresent(), "Table %s does not exist", tableName);
        Map<ColumnHandle, String> columnHandles = ImmutableBiMap.copyOf(metadata.getColumnHandles(tableHandle.get())).inverse();
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle.get(), Optional.<TupleDomain>absent());

        for (Partition partition : partitionResult.getPartitions()) {
            for (Entry<ColumnHandle, Comparable<?>> entry : partition.getTupleDomain().extractFixedValues().entrySet()) {
                ColumnHandle columnHandle = entry.getKey();
                String columnName = columnHandles.get(columnHandle);
                String value = entry.getValue() != null ? String.valueOf(entry.getValue()) : null;
                table.add(table.getTupleInfo().builder()
                        .append(catalogName)
                        .append(tableName.getSchemaName())
                        .append(tableName.getTableName())
                        .append(partitionNumber)
                        .append(columnName)
                        .append(value)
                        .build());
            }
            partitionNumber++;
        }
        return table.build();
    }

    private QualifiedTableName extractQualifiedTableName(String catalogName, Map<String, Object> filters)
    {
        Optional<String> schemaName = getFilterColumn(filters, "table_schema");
        Preconditions.checkArgument(schemaName.isPresent(), "filter is required for column: %s.%s", InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS, "table_schema");
        Optional<String> tableName = getFilterColumn(filters, "table_name");
        Preconditions.checkArgument(tableName.isPresent(), "filter is required for column: %s.%s", InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS, "table_name");
        return new QualifiedTableName(catalogName, schemaName.get(), tableName.get());
    }

    private QualifiedTablePrefix extractQualifiedTablePrefix(String catalogName, Map<String, Object> filters)
    {
        return new QualifiedTablePrefix(catalogName,
                getFilterColumn(filters, "table_schema"),
                getFilterColumn(filters, "table_name"));
    }

    private static Optional<String> getFilterColumn(Map<String, Object> filters, String columnName)
    {
        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            if (entry.getKey().equals(columnName)) {
                if (entry.getValue() instanceof String) {
                    return Optional.of((String) entry.getValue());
                }
                break;
            }
        }
        return Optional.absent();
    }
}
