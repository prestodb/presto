package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TableColumn
{
    private final QualifiedTableName table;
    private final String columnName;
    private final int ordinalPosition;
    private final TupleInfo.Type dataType;

    public TableColumn(QualifiedTableName table, String columnName, int ordinalPosition, TupleInfo.Type dataType)
    {
        this.table = checkTable(table);
        this.columnName = checkNotNull(columnName, "columName is null");
        checkArgument(ordinalPosition >= 1, "ordinal position must be at least one");
        this.ordinalPosition = ordinalPosition;
        this.dataType = checkNotNull(dataType, "dataType is null");
    }

    public QualifiedTableName getTable()
    {
        return table;
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
        return Objects.hashCode(table, columnName, ordinalPosition, dataType);
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
        return Objects.equal(table, o.table) &&
                Objects.equal(columnName, o.columnName) &&
                Objects.equal(ordinalPosition, o.ordinalPosition) &&
                Objects.equal(dataType, o.dataType);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("columnName", columnName)
                .add("ordinalPosition", ordinalPosition)
                .add("dataType", dataType)
                .toString();
    }
}
