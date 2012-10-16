package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class TableMetadata
{
    private final QualifiedName name;
    private final List<ColumnMetadata> columns;

    public TableMetadata(QualifiedName name, List<ColumnMetadata> columns)
    {
        this.name = name;
        this.columns = ImmutableList.copyOf(columns);
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }
}
