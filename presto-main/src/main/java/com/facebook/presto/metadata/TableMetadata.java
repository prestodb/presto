package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TableMetadata
{
    private final QualifiedTableName table;
    private final List<ColumnMetadata> columns;

    public TableMetadata(QualifiedTableName table, List<ColumnMetadata> columns)
    {
        this.table = checkNotNull(table, "table is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        checkArgument(!columns.isEmpty(), "columns is empty");
    }

    public QualifiedTableName getTable()
    {
        return table;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("columns", columns)
                .toString();
    }
}
