package com.facebook.presto.metadata;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TableMetadata
{
    private final QualifiedTableName table;
    private final List<ColumnMetadata> columns;
    private final Optional<TableHandle> tableHandle;

    public TableMetadata(QualifiedTableName table, List<ColumnMetadata> columns)
    {
        this(table, columns, Optional.<TableHandle>absent());
    }

    public TableMetadata(QualifiedTableName table, List<ColumnMetadata> columns, TableHandle tableHandle)
    {
        this(table, columns, Optional.of(checkNotNull(tableHandle, "tableHandle is null")));
    }

    private TableMetadata(QualifiedTableName table, List<ColumnMetadata> columns, Optional<TableHandle> tableHandle)
    {
        checkNotNull(table, "table is null");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "columns is empty");

        this.table = table;
        this.columns = ImmutableList.copyOf(columns);
        this.tableHandle = tableHandle;
    }

    public QualifiedTableName getTable()
    {
        return table;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    public Optional<TableHandle> getTableHandle()
    {
        return tableHandle;
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
