package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TableColumn
{
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String columnName;
    private final int ordinalPosition;
    private final TupleInfo.Type dataType;

    public TableColumn(String catalogName, String schemaName, String tableName, String columnName, int ordinalPosition, TupleInfo.Type dataType)
    {
        checkTableName(catalogName, schemaName, tableName);
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = checkNotNull(columnName, "columnName is null");
        checkArgument(ordinalPosition >= 1, "ordinal position must be at least one");
        this.ordinalPosition = ordinalPosition;
        this.dataType = checkNotNull(dataType, "dataType is null");
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

    public String getColumnName()
    {
        return columnName;
    }

    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public TupleInfo.Type getDataType()
    {
        return dataType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(
                catalogName, schemaName, tableName,
                columnName, ordinalPosition, dataType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TableColumn o = (TableColumn) obj;
        return Objects.equal(catalogName, o.catalogName) &&
                Objects.equal(schemaName, o.schemaName) &&
                Objects.equal(tableName, o.tableName) &&
                Objects.equal(columnName, o.columnName) &&
                Objects.equal(ordinalPosition, o.ordinalPosition) &&
                Objects.equal(dataType, o.dataType);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("catalogName", catalogName)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("columnName", columnName)
                .add("ordinalPosition", ordinalPosition)
                .add("dataType", dataType)
                .toString();
    }
}
