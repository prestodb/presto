package com.facebook.presto.common.clustering;

import com.facebook.presto.common.type.Type;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;

// Assume: We have 100 values to represent 100 percentiles
// for the column for now.
public final class Distribution
{
    private List<Pair<Integer, Object>> distribution;
    // This is used to convert to Java type for intervals.
    private Type columnType;

    public Distribution(List<Pair<Integer, Object>> distribution, Type columnType)
    {
        this.distribution = distribution;
        this.columnType = columnType;
    }

    // NOW assume we always have 100 intervals for each column.
    // We are able to generate arbitrary number of intervals
    // when we need to.
    // TODO: Support arbitrary number of intervals.
    public ColumnInterval getIntervals(int intervalCount)
    {
        return getDefaultInterval();
    }

    public ColumnInterval getDefaultInterval()
    {
        List<Object> intervals = new ArrayList<>();
        for (Pair<Integer, Object> bar : distribution) {
            intervals.add(bar.getValue());
        }
        return new ColumnInterval(intervals, columnType);
    }
}
