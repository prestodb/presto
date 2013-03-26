package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.metadata.InformationSchemaMetadata.informationSchemaColumnIndex;
import static com.facebook.presto.metadata.InformationSchemaMetadata.informationSchemaTupleInfo;
import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;

public class InformationSchemaData
{
    private final Metadata metadata;

    @Inject
    public InformationSchemaData(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    public InternalTable getInternalTable(QualifiedTableName table, Map<InternalColumnHandle, String> filters)
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

    private InternalTable buildColumns(String catalogName, Map<InternalColumnHandle, String> filters)
    {
        TupleInfo tupleInfo = informationSchemaTupleInfo(TABLE_COLUMNS);
        InternalTable.Builder table = InternalTable.builder(tupleInfo);
        for (TableColumn column : getColumnsList(catalogName, filters)) {
            table.add(tupleInfo.builder()
                    .append(column.getTable().getCatalogName())
                    .append(column.getTable().getSchemaName())
                    .append(column.getTable().getTableName())
                    .append(column.getColumnName())
                    .append(column.getOrdinalPosition())
                    .appendNull()
                    .append("YES")
                    .append(column.getDataType().getName())
                    .build());
        }
        return table.build();
    }

    private List<TableColumn> getColumnsList(String catalogName, Map<InternalColumnHandle, String> filters)
    {
        return metadata.listTableColumns(catalogName,
                fromNullable(getFilterColumn(filters, TABLE_COLUMNS, "table_schema")),
                fromNullable(getFilterColumn(filters, TABLE_COLUMNS, "table_name")));
    }

    private InternalTable buildTables(String catalogName, Map<InternalColumnHandle, String> filters)
    {
        TupleInfo tupleInfo = informationSchemaTupleInfo(TABLE_TABLES);
        InternalTable.Builder table = InternalTable.builder(tupleInfo);
        for (QualifiedTableName name : getTablesList(catalogName, filters)) {
            table.add(tupleInfo.builder()
                    .append(name.getCatalogName())
                    .append(name.getSchemaName())
                    .append(name.getTableName())
                    .append("BASE TABLE")
                    .build());
        }
        return table.build();
    }

    private List<QualifiedTableName> getTablesList(String catalogName, Map<InternalColumnHandle, String> filters)
    {
        return metadata.listTables(catalogName, Optional.fromNullable(getFilterColumn(filters, TABLE_TABLES, "table_schema")));
    }

    private InternalTable buildFunctions()
    {
        TupleInfo tupleInfo = informationSchemaTupleInfo(TABLE_INTERNAL_FUNCTIONS);
        InternalTable.Builder table = InternalTable.builder(tupleInfo);
        for (FunctionInfo function : metadata.listFunctions()) {
            Iterable<String> arguments = transform(function.getArgumentTypes(), TupleInfo.Type.nameGetter());
            table.add(tupleInfo.builder()
                    .append(function.getName().toString())
                    .append(Joiner.on(", ").join(arguments))
                    .append(function.getReturnType().getName())
                    .build());
        }
        return table.build();
    }

    private InternalTable buildPartitions(String catalogName, Map<InternalColumnHandle, String> filters)
    {
        String schemaName = requiredFilterColumn(filters, TABLE_INTERNAL_PARTITIONS, "table_schema");
        String tableName = requiredFilterColumn(filters, TABLE_INTERNAL_PARTITIONS, "table_name");

        TupleInfo tupleInfo = informationSchemaTupleInfo(TABLE_INTERNAL_PARTITIONS);
        InternalTable.Builder table = InternalTable.builder(tupleInfo);
        int partitionNumber = 1;
        List<Map<String, String>> partitions = metadata.listTablePartitionValues(catalogName,
                fromNullable(schemaName),
                fromNullable(tableName));

        for (Map<String, String> partition : partitions) {
            for (Map.Entry<String, String> entry : partition.entrySet()) {
                table.add(tupleInfo.builder()
                        .append(catalogName)
                        .append(schemaName)
                        .append(tableName)
                        .append(partitionNumber)
                        .append(entry.getKey())
                        .append(entry.getValue())
                        .build());
            }
            partitionNumber++;
        }
        return table.build();
    }

    private static String requiredFilterColumn(Map<InternalColumnHandle, String> filters, String tableName, String columnName)
    {
        String value = getFilterColumn(filters, tableName, columnName);
        checkArgument(value != null, "filter is required for column: %s.%s", tableName, columnName);
        return value;
    }

    private static String getFilterColumn(Map<InternalColumnHandle, String> filters, String tableName, String columnName)
    {
        int index = informationSchemaColumnIndex(tableName, columnName);
        for (Map.Entry<InternalColumnHandle, String> entry : filters.entrySet()) {
            if (entry.getKey().getColumnIndex() == index) {
                return entry.getValue();
            }
        }
        return null;
    }
}
