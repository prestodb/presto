package com.facebook.presto.hive.clustering;

import com.erenck.mortonlib.Morton3D;
import com.facebook.presto.common.Page;
import com.facebook.presto.hive.HiveType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MortonCode
{
    public MortonCode(Map<String, Interval> intervals) {
        this.intervals = intervals;
    }

    private Map<String, Interval> intervals;

    public long getMortonCode(
            List<TypeInfo> types, List<String> columnNames, Page page, int position)
    {
        // For prototyping purpose, we only encode integer columns.
        checkColumnTypes(types);

        Morton3D mortonCurve = new Morton3D();

        List<Integer> indices = getIntervalIndices(
                columnNames, intervals, page, position
        );

        long mortonCode = mortonCurve.encode(
                indices.get(0),
                indices.get(1),
                indices.size() == 3? indices.get(2) : 0
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
    private static void checkColumnTypes(List<TypeInfo> types)
    {
        for (TypeInfo typeInfo : types)
        {
            if (typeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UnsupportedOperationException(
                        "Clustering supports only primitive data types!"
                );
            }

            PrimitiveTypeInfo pTypeInfo = (PrimitiveTypeInfo) typeInfo;
            PrimitiveObjectInspector.PrimitiveCategory pCategory = pTypeInfo.getPrimitiveCategory();

            if (pCategory == PrimitiveObjectInspector.PrimitiveCategory.SHORT)
                return;
            if (pCategory != PrimitiveObjectInspector.PrimitiveCategory.INT)
                return;
            if (pCategory != PrimitiveObjectInspector.PrimitiveCategory.LONG)
                return;

            throw new UnsupportedOperationException(
                    "Clustering supports only integer data types!"
            );
        }
    }

    private static List<Integer> getIntervalIndices(
            List<String> columnNames,
            Map<String, Interval> columnIntervals,
            Page page,
            int position) {
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); ++i) {
            String columnName = columnNames.get(i);
            Interval intervals = columnIntervals.get(columnName);
            int index = getInvervalIndex(intervals, i, page, position);
            indices.add(index);
        }
        return indices;
    }

    private static int getInvervalIndex(Interval intervals, int idx, Page page, int position)
    {
        List<Object> splittingValues = intervals.getSplittingValues();
        HiveType columnType = intervals.getColumnType();

        int i = 0;
        for (; i < splittingValues.size(); ++i) {
            Object splitValue = splittingValues.get(i);
            if (compare(splitValue, columnType, idx, page, position)) {
                return i;
            }
        }
        return i;
    }

    static boolean compare(Object splittingValue, HiveType columnType, int idx, Page page, int position)
            throws IllegalArgumentException
    {
        if (columnType == HiveType.HIVE_INT ||
                columnType == HiveType.HIVE_LONG ||
                columnType == HiveType.HIVE_SHORT
        ) {
            return (Long) splittingValue >= page.getBlock(idx).getLong(position);
        } else if (columnType == HiveType.HIVE_DOUBLE ||
                columnType == HiveType.HIVE_FLOAT
        ) {
            // How to get double value?
            return (Double) splittingValue >= page.getBlock(idx).getLong(position);
        } else if (columnType == HiveType.HIVE_STRING){
            // How to get string value?
            int r = ((String) splittingValue).compareTo(
                    (String.valueOf(page.getBlock(idx).getLong(position))));
            return r >= 0;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Hive clustering does not support column type: %s",
                            columnType.toString()
                    )
            );
        }
    }

}
