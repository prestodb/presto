package com.facebook.presto.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SchemaTableMetadata
{
    private final SchemaTableName table;
    private final List<ColumnMetadata> columns;
    private final List<String> partitionKeys;

    public SchemaTableMetadata(SchemaTableName table, List<ColumnMetadata> columns)
    {
        this(table, columns, Collections.<String>emptyList());
    }

    public SchemaTableMetadata(SchemaTableName table, List<ColumnMetadata> columns, List<String> partitionKeys)
    {
        if (table == null) {
            throw new NullPointerException("table is null or empty");
        }
        if (columns == null) {
            throw new NullPointerException("columns is null");
        }
        if (partitionKeys == null) {
            throw new NullPointerException("partitionKeys is null");
        }

        this.table = table;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.partitionKeys = Collections.unmodifiableList(new ArrayList<>(partitionKeys));
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
        final StringBuilder sb = new StringBuilder("SchemaTableMetadata{");
        sb.append("table=").append(table);
        sb.append(", columns=").append(columns);
        sb.append(", partitionKeys=").append(partitionKeys);
        sb.append('}');
        return sb.toString();
    }
}
