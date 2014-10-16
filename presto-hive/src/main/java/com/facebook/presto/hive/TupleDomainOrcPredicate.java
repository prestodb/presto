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
package com.facebook.presto.hive;

import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.metadata.BucketStatistics;
import com.facebook.presto.orc.metadata.ColumnStatistics;
import com.facebook.presto.orc.metadata.RangeStatistics;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.slice.Slices.utf8Slice;

public class TupleDomainOrcPredicate<C>
        implements OrcPredicate
{
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private final TupleDomain<C> tupleDomain;
    private final List<ColumnReference<C>> columnReferences;

    public TupleDomainOrcPredicate(TupleDomain<C> tupleDomain, List<ColumnReference<C>> columnReferences)
    {
        this.tupleDomain = checkNotNull(tupleDomain, "tupleDomain is null");
        checkNotNull(columnReferences, "columnReferences is null");

        // verify all columns in tuple domain have a column reference
        Set<C> filteredColumns = tupleDomain.getDomains().keySet();
        checkArgument(ImmutableSet.copyOf(transform(columnReferences, ColumnReference.<C>columnGetter())).containsAll(filteredColumns),
                "Tuple domain references columns not in column reference list");

        // only keep columns used in the tuple domain
        this.columnReferences = ImmutableList.copyOf(filter(columnReferences, compose(in(filteredColumns), ColumnReference.<C>columnGetter())));
    }

    @Override
    public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByHiveColumnIndex)
    {
        ImmutableMap.Builder<C, Domain> domains = ImmutableMap.builder();

        for (ColumnReference<C> columnReference : columnReferences) {
            ColumnStatistics columnStatistics = statisticsByHiveColumnIndex.get(columnReference.getOrdinal());
            if (columnStatistics == null) {
                // no stats for column
                return true;
            }

            domains.put(columnReference.getColumn(), getDomain(columnReference.getType(), numberOfRows, columnStatistics));
        }
        TupleDomain<C> stripeDomain = TupleDomain.withColumnDomains(domains.build());

        return tupleDomain.overlaps(stripeDomain);
    }

    private static Domain getDomain(Type type, long rowCount, ColumnStatistics columnStatistics)
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

    public static class ColumnReference<C>
    {
        private final C column;
        private final int ordinal;
        private final Type type;

        public ColumnReference(C column, int ordinal, Type type)
        {
            this.column = checkNotNull(column, "column is null");
            checkArgument(ordinal >= 0, "ordinal is negative");
            this.ordinal = ordinal;
            this.type = checkNotNull(type, "type is null");
        }

        public C getColumn()
        {
            return column;
        }

        public int getOrdinal()
        {
            return ordinal;
        }

        public Type getType()
        {
            return type;
        }

        public static <C> Function<ColumnReference<C>, C> columnGetter()
        {
            return new Function<ColumnReference<C>, C>()
            {
                @Override
                public C apply(ColumnReference<C> input)
                {
                    return input.getColumn();
                }
            };
        }
    }
}
