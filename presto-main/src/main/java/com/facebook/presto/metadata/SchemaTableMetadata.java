package com.facebook.presto.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SchemaTableMetadata
{
    private final SchemaTableName table;
    private final List<ColumnMetadata> columns;
    private final List<String> partitionKeys;

    public SchemaTableMetadata(SchemaTableName table, List<ColumnMetadata> columns)
    {
        this(table, columns, ImmutableList.<String>of());
    }

    public SchemaTableMetadata(SchemaTableName table, List<ColumnMetadata> columns, List<String> partitionKeys)
    {
        this.table = checkNotNull(table, "table is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        checkArgument(!columns.isEmpty(), "columns is empty");
        this.partitionKeys = checkNotNull(partitionKeys, "partitionKeys is null");
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    public List<String> getPartitionKeys()
    {
        return partitionKeys;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("columns", columns)
                .add("partitionKeys", partitionKeys)
                .toString();
    }
}
