package com.facebook.presto.common.clustering;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IntervalExtractor
{
    private IntervalExtractor() {}

    // TODO:
    // Currently each column contains 99 percerntiles in string types.
    // Several things can be improved:
    // 1. For different columns, do we have better ways to store them in table properties?
    // 2. To support bucket count, we need to allow users to give more than 100 percentiles
    //    for each column if needed.
    // 3. Use native Interval structure to store the intervals.
    // 4. The order of columns in distributions may not match with bucketBy columns. We should
    //    choose more safe structures to store distribution.
    public static Map<String, ColumnInterval> extractIntervals(
            List<String> distributions, List<String> columns, List<Type> columnTypes)
    {
        Map<String, ColumnInterval> intervals = new HashMap<>();
        for (int i = 0; i < columns.size(); ++i)
        {
            ColumnInterval interval = new ColumnInterval(
                    extractSplittingValues(distributions, i * 9, (i + 1) * 9, columnTypes.get(i)),
                    columnTypes.get(i)
            );
            intervals.put(columns.get(i), interval);
        }
        return intervals;
    }

    private static List<Object> extractSplittingValues(List<String> distribution, int start, int end, Type columnType)
    {
        List<Object> splittingValues = new ArrayList<>();
        for (int i = start; i < end; ++i)
        {
            if (TypeUtils.isExactNumericType(columnType)) {
                splittingValues.add(Integer.parseInt(distribution.get(i)));
            } else if (TypeUtils.isNumericType(columnType)) {
                splittingValues.add(Double.parseDouble(distribution.get(i)));
            } else {
                splittingValues.add(distribution.get(i));
            }
        }
        return splittingValues;
    }

    // Give each column the same filtering power.
    // TODO: Based on maxFileCount, we can calculate the number of clusters for each dimension
    // columns by factoring the maxFileCount.
    public static List<Integer> generateIntervalCounts(int columnCount, int maxFileCount) {
        return null;
    }


}
