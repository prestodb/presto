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

    public static Map<String, List<Long>> generateIntervals(
            Map<String, List<Long>> dataDistribution, int maxFileCount)
    {
        Map<String, List<Long>> columnIntervals = new HashMap<>();
        Iterator<Map.Entry<String, List<Long>>> it = dataDistribution.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, List<Long>> entry = it.next();

        }

        return columnIntervals;
    }

    // Give each column the same filtering power.
    public static List<Integer> generateIntervalCounts(int columnCount, int maxFileCount) {
        return null;
    }


}
