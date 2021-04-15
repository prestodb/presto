package com.facebook.presto.common.clustering;

import com.erenck.mortonlib.Morton3D;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MortonCode
{
    public MortonCode(Map<String, ColumnInterval> intervals) {
        this.intervals = intervals;
    }

    private Map<String, ColumnInterval> intervals;

    public long getMortonCode(
            List<Type> types, List<String> columnNames, Page page, int position)
    {
        // For prototyping purpose, we only encode integer columns.
        checkColumnTypes(types);

        Morton3D mortonCurve = new Morton3D();

        List<Integer> indices = getIntervalIndices(columnNames, intervals, page, position);

        // TODO: Create our own Z-order library.
        // API of this library has a few limitations:
        // 1. has and only has three parameters.
        // 2. for two or one dimensions, it will generate the bucket number is larger than
        //    the bucket number.

        if (indices.size() == 1) {
            return indices.get(0);
        }

        long mortonCode = mortonCurve.encode(
                indices.get(0),
                indices.size() >= 2? indices.get(1) : 0,
                indices.size() >= 3? indices.get(2) : 0
        );

        return mortonCode;
    }

    // TODO: 3/5/21 mortonCurve should be a singleton for a page.
    private static int[] getColumnValues(long clusterCode)
    {
        Morton3D mortonCurve = new Morton3D();
        return mortonCurve.decode(clusterCode);
    }

    // TODO: 3/5/21 This should be checked only once for one page instead of each row.
    private static void checkColumnTypes(List<Type> types)
    {
        for (Type type : types) {
            if (!TypeUtils.isExactNumericType(type) && !TypeUtils.isNumericType(type)) {
                throw new UnsupportedOperationException(
                        "Clustering supports only integer and double types!"
                );
            }
        }
    }

    private static List<Integer> getIntervalIndices(
            List<String> columnNames,
            Map<String, ColumnInterval> columnIntervals,
            Page page,
            int position) {
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); ++i) {
            String columnName = columnNames.get(i);
            ColumnInterval intervals = columnIntervals.get(columnName);
            int index = getInvervalIndex(intervals, i, page, position);
            indices.add(index);
        }
        return indices;
    }

    private static int getInvervalIndex(ColumnInterval intervals, int idx, Page page, int position)
    {
        List<Object> splittingValues = intervals.getSplittingValues();
        Type columnType = intervals.getColumnType();

        int i = 0;
        for (; i < splittingValues.size(); ++i) {
            Object splitValue = splittingValues.get(i);
            if (compare(splitValue, columnType, idx, page, position)) {
                return i;
            }
        }
        return i;
    }

    static boolean compare(Object splittingValue, Type columnType, int idx, Page page, int position)
    {
        if (TypeUtils.isExactNumericType(columnType)) {
            return (Integer) splittingValue >= page.getBlock(idx).getLong(position);
        } else {
            return (Double) splittingValue >= page.getBlock(idx).getLong(position);
        }
    }
}
