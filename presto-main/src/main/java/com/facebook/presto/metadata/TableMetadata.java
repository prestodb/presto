package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;

public class TableMetadata
{
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final List<ColumnMetadata> columns;

    public TableMetadata(String catalogName, String schemaName, String tableName, List<ColumnMetadata> columns)
    {
        checkNotNull(emptyToNull(catalogName), "catalogName is null or empty");
        checkNotNull(emptyToNull(schemaName), "schemaName is null or empty");
        checkNotNull(emptyToNull(tableName), "tableName is null or empty");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "columns is empty");

        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = ImmutableList.copyOf(columns);
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }
}
