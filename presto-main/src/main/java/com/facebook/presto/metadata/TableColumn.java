package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
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
    private final long columnId;

    public TableColumn(QualifiedTableName table, String columnName, int ordinalPosition, Type dataType, long columnId)
    {
        this.table = checkTable(table);
        this.columnName = checkNotNull(columnName, "columnName is null");
        checkArgument(ordinalPosition >= 0, "ordinal position is negative");
        this.ordinalPosition = ordinalPosition;
        this.dataType = checkNotNull(dataType, "dataType is null");
        this.columnId = columnId;
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

    public long getColumnId()
    {
        return columnId;
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
