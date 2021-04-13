package com.facebook.presto.common.clustering;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IntervalExtractor
{
    private IntervalExtractor() {}

    public static Map<String, ColumnInterval> extractIntervals(
            Map<String, Distribution> distributions, int maxFileCount)
    {
        Map<String, ColumnInterval> columnIntervals = new HashMap<>();
        for (Map.Entry<String, Distribution> columnDistribution : distributions.entrySet()) {
            ColumnInterval interval = columnDistribution.getValue().getDefaultInterval();
            columnIntervals.put(columnDistribution.getKey(), interval);
        }
        return columnIntervals;
    }

    // Give each column the same filtering power.
    // TODO: Based on maxFileCount, we can calculate the number of clusters for each dimension
    // columns by factoring the maxFileCount.
    public static List<Integer> generateIntervalCounts(int columnCount, int maxFileCount) {
        return null;
    }


}
