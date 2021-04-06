package com.facebook.presto.hive.clustering;

import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class IntervalExtractor
{
    private IntervalExtractor() {}

    public static Map<String, Interval> extractIntervals(
            Map<String, Distribution> distributions, int maxFileCount)
    {
        Map<String, Interval> columnIntervals = new HashMap<>();
        for (Map.Entry<String, Distribution> columnDistribution : distributions.entrySet()) {
            Interval interval = columnDistribution.getValue().getDefaultInterval();
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
