package com.facebook.presto.hive.clustering;

import com.facebook.presto.hive.HiveType;
import javafx.util.Pair;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.List;

// Assume: We have 100 values to represent 100 percentiles
// for the column for now.
public final class Distribution
{
    private List<Pair<Integer, Object>> distribution;
    // This is used to convert to Java type for intervals.
    private HiveType columnType;

    public Distribution(List<Pair<Integer, Object>> distribution, HiveType columnType)
    {
        this.distribution = distribution;
        this.columnType = columnType;
    }

    // NOW assume we always have 100 intervals for each column.
    // We are able to generate arbitrary number of intervals
    // when we need to.
    // TODO: Support arbitrary number of intervals.
    public Interval getIntervals(int intervalCount)
    {
        return getDefaultInterval();
    }

    public Interval getDefaultInterval()
    {
        List<Object> intervals = new ArrayList<>();
        for (Pair<Integer, Object> bar : distribution) {
            intervals.add(bar.getValue());
        }
        return new Interval(intervals, columnType);
    }
}
