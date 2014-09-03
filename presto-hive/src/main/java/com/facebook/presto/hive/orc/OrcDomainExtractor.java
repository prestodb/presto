/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.orc.metadata.BucketStatistics;
import com.facebook.presto.hive.orc.metadata.ColumnStatistics;
import com.facebook.presto.hive.orc.metadata.RangeStatistics;
import com.facebook.presto.hive.orc.metadata.RowGroupIndex;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;

public final class OrcDomainExtractor
{
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private OrcDomainExtractor()
    {
    }

    public static List<TupleDomain<HiveColumnHandle>> extractDomain(
            TypeManager typeManager,
            Map<HiveColumnHandle, Integer> columnHandles,
            int rowsInStripe,
            int rowsInRowGroup,
            List<List<RowGroupIndex>> columnIndexes)
    {
        ImmutableList.Builder<TupleDomain<HiveColumnHandle>> rowGroupTupleDomains = ImmutableList.builder();

        int rowGroup = 0;
        for (int remainingRows = rowsInStripe; remainingRows > 0; remainingRows -= rowsInRowGroup) {
            int rows = Math.min(remainingRows, rowsInRowGroup);
            TupleDomain<HiveColumnHandle> rowGroupTupleDomain = extractDomain(typeManager, columnHandles, columnIndexes, rowGroup, rows);
            rowGroupTupleDomains.add(rowGroupTupleDomain);
            rowGroup++;
        }

        return rowGroupTupleDomains.build();
    }

    public static TupleDomain<HiveColumnHandle> extractDomain(
            TypeManager typeManager,
            Map<HiveColumnHandle, Integer> columnHandles,
            List<List<RowGroupIndex>> columnIndexes,
            int rowGroup,
            long rowCount)
    {
        ImmutableMap.Builder<HiveColumnHandle, Domain> domains = ImmutableMap.builder();
        for (Entry<HiveColumnHandle, Integer> entry : columnHandles.entrySet()) {
            HiveColumnHandle columnHandle = entry.getKey();
            Integer streamIndex = entry.getValue();

            RowGroupIndex rowGroupIndex = columnIndexes.get(streamIndex).get(rowGroup);
            Domain domain = getDomain(toPrestoType(typeManager, columnHandle.getHiveType()), rowCount, rowGroupIndex.getColumnStatistics());
            domains.put(columnHandle, domain);
        }
        return TupleDomain.withColumnDomains(domains.build());
    }

    public static TupleDomain<HiveColumnHandle> extractDomain(
            TypeManager typeManager,
            Map<HiveColumnHandle, Integer> columnHandles,
            long rowCount,
            List<ColumnStatistics> stripeColumnStatistics)
    {
        ImmutableMap.Builder<HiveColumnHandle, Domain> domains = ImmutableMap.builder();
        for (Entry<HiveColumnHandle, Integer> entry : columnHandles.entrySet()) {
            HiveColumnHandle columnHandle = entry.getKey();
            Integer columnIndex = entry.getValue();

            Domain domain = getDomain(toPrestoType(typeManager, columnHandle.getHiveType()), rowCount, stripeColumnStatistics.get(columnIndex));
            domains.put(columnHandle, domain);
        }
        return TupleDomain.withColumnDomains(domains.build());
    }

    private static Type toPrestoType(TypeManager typeManager, HiveType hiveType)
    {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(hiveType.getHiveTypeName());
        switch (typeInfo.getCategory()) {
            case MAP:
            case LIST:
            case STRUCT:
                return VARCHAR;
            case PRIMITIVE:
                PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
                switch (primitiveTypeInfo.getPrimitiveCategory()) {
                    case BOOLEAN:
                        return BOOLEAN;
                    case BYTE:
                    case SHORT:
                    case INT:
                    case LONG:
                        return BIGINT;
                    case FLOAT:
                    case DOUBLE:
                        return DOUBLE;
                    case STRING:
                        return VARCHAR;
                    case DATE:
                        return DATE;
                    case TIMESTAMP:
                        return TIMESTAMP;
                    case BINARY:
                        return VARBINARY;
                }
        }
        throw new IllegalArgumentException("Unsupported hive type " + hiveType);
    }

    public static Domain getDomain(Type type, long rowCount, ColumnStatistics columnStatistics)
    {
        Class<?> boxedJavaType = Primitives.wrap(type.getJavaType());
        if (rowCount == 0) {
            return Domain.none(boxedJavaType);
        }

        if (columnStatistics == null) {
            return Domain.all(boxedJavaType);
        }

        if (columnStatistics.hasNumberOfValues() && columnStatistics.getNumberOfValues() == 0) {
            return Domain.onlyNull(boxedJavaType);
        }

        boolean hasNullValue = columnStatistics.getNumberOfValues() != rowCount;

        if (boxedJavaType == Boolean.class && columnStatistics.getBucketStatistics() != null) {
            BucketStatistics bucketStatistics = columnStatistics.getBucketStatistics();

            boolean hasTrueValues = (bucketStatistics.getCount(0) != 0);
            boolean hasFalseValues = (columnStatistics.getNumberOfValues() != bucketStatistics.getCount(0));
            if (hasTrueValues && hasFalseValues) {
                return Domain.all(Boolean.class);
            }
            else if (hasTrueValues) {
                return Domain.create(SortedRangeSet.singleValue(true), hasNullValue);
            }
            else if (hasFalseValues) {
                return Domain.create(SortedRangeSet.singleValue(false), hasNullValue);
            }
        }
        else if (boxedJavaType == Long.class && columnStatistics.getIntegerStatistics() != null) {
            return createDomain(boxedJavaType, hasNullValue, columnStatistics.getIntegerStatistics());
        }
        else if (boxedJavaType == Double.class && columnStatistics.getDateStatistics() != null) {
            return createDomain(boxedJavaType, hasNullValue, columnStatistics.getDoubleStatistics());
        }
        else if (boxedJavaType == Slice.class && columnStatistics.getStringStatistics() != null) {
            return createDomain(boxedJavaType, hasNullValue, columnStatistics.getStringStatistics(), new Function<String, Slice>()
            {
                @Override
                public Slice apply(String string)
                {
                    return utf8Slice(string);
                }
            });
        }
        else if (boxedJavaType == Long.class && columnStatistics.getDateStatistics() != null) {
            return createDomain(boxedJavaType, hasNullValue, columnStatistics.getDateStatistics(), new Function<Integer, Long>()
            {
                @Override
                public Long apply(Integer days)
                {
                    return days * MILLIS_IN_DAY;
                }
            });
        }
        return Domain.create(SortedRangeSet.all(boxedJavaType), hasNullValue);
    }

    private static <T extends Comparable<T>> Domain createDomain(Class<?> boxedJavaType, boolean hasNullValue, RangeStatistics<T> rangeStatistics)
    {
        return createDomain(boxedJavaType, hasNullValue, rangeStatistics, Functions.<T>identity());
    }

    private static <F, T extends Comparable<T>> Domain createDomain(Class<?> boxedJavaType, boolean hasNullValue, RangeStatistics<F> rangeStatistics, Function<F, T> function)
    {
        F min = rangeStatistics.getMin();
        F max = rangeStatistics.getMax();

        if (min != null && max != null) {
            return Domain.create(SortedRangeSet.of(Range.range(function.apply(min), true, function.apply(max), true)), hasNullValue);
        }
        else if (max != null) {
            return Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(function.apply(max))), hasNullValue);
        }
        else if (min != null) {
            return Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(function.apply(min))), hasNullValue);
        }
        return Domain.create(SortedRangeSet.all(boxedJavaType), hasNullValue);
    }
}
