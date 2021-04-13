package com.facebook.presto.hive.clustering;

import com.facebook.presto.hive.HiveType;

import java.util.List;

public class Interval
{
    private List<Object> splittingValues;

    private HiveType columnType;

    public Interval(List<Object> splittingValues, HiveType columnType) {
        this.splittingValues = splittingValues;
        this.columnType = columnType;
    }

    public List<Object> getSplittingValues() {
        return splittingValues;
    }

    public HiveType getColumnType()
    {
        return columnType;
    }
}
