package com.facebook.presto.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableMetadata
{
    private final SchemaTableName table;
    private final List<ColumnMetadata> columns;

    public TableMetadata(SchemaTableName table, List<ColumnMetadata> columns)
    {
        if (table == null) {
            throw new NullPointerException("table is null or empty");
        }
        if (columns == null) {
            throw new NullPointerException("columns is null");
        }

        this.table = table;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    }

    public SchemaTableName getTable()
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
        final StringBuilder sb = new StringBuilder("SchemaTableMetadata{");
        sb.append("table=").append(table);
        sb.append(", columns=").append(columns);
        sb.append('}');
        return sb.toString();
    }
}
