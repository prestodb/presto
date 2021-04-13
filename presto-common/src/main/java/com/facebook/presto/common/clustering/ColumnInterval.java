package com.facebook.presto.common.clustering;

import com.facebook.presto.common.type.Type;

import java.util.List;

public class ColumnInterval
{
    private List<Object> splittingValues;

    private Type columnType;

    public ColumnInterval(List<Object> splittingValues, Type columnType) {
        this.splittingValues = splittingValues;
        this.columnType = columnType;
    }

    public List<Object> getSplittingValues() {
        return splittingValues;
    }

    public Type getColumnType()
    {
        return columnType;
    }
}
