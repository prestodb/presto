package com.facebook.presto.common.clustering;

import javax.swing.JInternalFrame;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IntervalExtractor
{
    private IntervalExtractor() {}

    public static Map<String, ColumnInterval> extractIntervals(
            List<String> distributions, List<String> columns)
    {
        Map<String, ColumnInterval> intervals = new HashMap<>();
        return intervals;
    }

    // Give each column the same filtering power.
    // TODO: Based on maxFileCount, we can calculate the number of clusters for each dimension
    // columns by factoring the maxFileCount.
    public static List<Integer> generateIntervalCounts(int columnCount, int maxFileCount) {
        return null;
    }


}
