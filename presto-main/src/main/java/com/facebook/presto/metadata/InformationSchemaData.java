package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableBiMap;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.metadata.InformationSchemaMetadata.informationSchemaTableColumns;
import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.facebook.presto.tuple.TupleInfo.Type.fromColumnType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;

public class InformationSchemaData
{
    private final Metadata metadata;
    private final SplitManager splitManager;

    @Inject
    public InformationSchemaData(Metadata metadata, SplitManager splitManager)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
    }

    public InternalTable getInternalTable(QualifiedTableName table, Map<InternalColumnHandle, Object> filters)
    {
        checkTable(table);
        checkArgument(table.getSchemaName().equals(INFORMATION_SCHEMA), "schema is not %s", INFORMATION_SCHEMA);

        switch (table.getTableName()) {
            case TABLE_COLUMNS:
                return buildColumns(table.getCatalogName(), filters);
            case TABLE_TABLES:
                return buildTables(table.getCatalogName(), filters);
            case TABLE_INTERNAL_FUNCTIONS:
                return buildFunctions();
            case TABLE_INTERNAL_PARTITIONS:
                return buildPartitions(table.getCatalogName(), filters);
        }

        throw new IllegalArgumentException(format("table does not exist: %s", table));
    }

    private InternalTable buildColumns(String catalogName, Map<InternalColumnHandle, Object> filters)
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_COLUMNS));
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

    private Map<QualifiedTableName, List<ColumnMetadata>> getColumnsList(String catalogName, Map<InternalColumnHandle, Object> filters)
    {
        return metadata.listTableColumns(extractQualifiedTablePrefix(catalogName, filters));
    }

    private InternalTable buildTables(String catalogName, Map<InternalColumnHandle, Object> filters)
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_TABLES));
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

    private List<QualifiedTableName> getTablesList(String catalogName, Map<InternalColumnHandle, Object> filters)
    {
        return metadata.listTables(extractQualifiedTablePrefix(catalogName, filters));
    }

    private InternalTable buildFunctions()
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_INTERNAL_FUNCTIONS));
        for (FunctionInfo function : metadata.listFunctions()) {
            Iterable<String> arguments = transform(function.getArgumentTypes(), TupleInfo.Type.nameGetter());
            table.add(table.getTupleInfo().builder()
                    .append(function.getName().toString())
                    .append(Joiner.on(", ").join(arguments))
                    .append(function.getReturnType().getName())
                    .build());
        }
        return table.build();
    }

    private InternalTable buildPartitions(String catalogName, Map<InternalColumnHandle, Object> filters)
    {
        QualifiedTableName tableName = extractQualifiedTableName(catalogName, filters);

        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_INTERNAL_PARTITIONS));
        int partitionNumber = 1;

        Optional<TableHandle> tableHandle = metadata.getTableHandle(tableName);
        checkArgument(tableHandle.isPresent(), "Table %s does not exist", tableName);
        Map<ColumnHandle, String> columnHandles = ImmutableBiMap.copyOf(metadata.getColumnHandles(tableHandle.get())).inverse();
        List<Partition> partitions = splitManager.getPartitions(tableHandle.get(), Optional.<Map<ColumnHandle, Object>>absent());

        for (Partition partition : partitions) {
            for (Map.Entry<ColumnHandle, String> entry : partition.getKeys().entrySet()) {
                ColumnHandle columnHandle = entry.getKey();
                String columnName = columnHandles.get(columnHandle);
                table.add(table.getTupleInfo().builder()
                        .append(catalogName)
                        .append(tableName.getSchemaName())
                        .append(tableName.getTableName())
                        .append(partitionNumber)
                        .append(columnName)
                        .append(entry.getValue())
                        .build());
            }
            partitionNumber++;
        }
        return table.build();
    }

    private QualifiedTableName extractQualifiedTableName(String catalogName, Map<InternalColumnHandle, Object> filters)
    {
        Optional<String> schemaName = getFilterColumn(filters, "table_schema");
        checkArgument(schemaName.isPresent(), "filter is required for column: %s.%s", TABLE_INTERNAL_PARTITIONS, "table_schema");
        Optional<String> tableName = getFilterColumn(filters, "table_name");
        checkArgument(schemaName.isPresent(), "filter is required for column: %s.%s", TABLE_INTERNAL_PARTITIONS, "table_name");
        return new QualifiedTableName(catalogName, schemaName.get(), tableName.get());
    }

    private QualifiedTablePrefix extractQualifiedTablePrefix(String catalogName, Map<InternalColumnHandle, Object> filters)
    {
        return new QualifiedTablePrefix(catalogName,
                getFilterColumn(filters, "table_schema"),
                getFilterColumn(filters, "table_name"));
    }

    private static Optional<String> getFilterColumn(Map<InternalColumnHandle, Object> filters, String columnName)
    {
        for (Map.Entry<InternalColumnHandle, Object> entry : filters.entrySet()) {
            if (entry.getKey().getColumnName().equals(columnName)) {
                if (entry.getValue() instanceof String) {
                    return Optional.of((String) entry.getValue());
                }
                break;
            }
        }
        return Optional.absent();
    }
}
