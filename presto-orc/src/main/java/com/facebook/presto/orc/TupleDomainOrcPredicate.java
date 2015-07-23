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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.BooleanStatistics;
import com.facebook.presto.orc.metadata.ColumnStatistics;
import com.facebook.presto.orc.metadata.RangeStatistics;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TupleDomainOrcPredicate<C>
        implements OrcPredicate
{
    private final TupleDomain<C> effectivePredicate;
    private final List<ColumnReference<C>> columnReferences;

    public TupleDomainOrcPredicate(TupleDomain<C> effectivePredicate, List<ColumnReference<C>> columnReferences)
    {
        this.effectivePredicate = checkNotNull(effectivePredicate, "effectivePredicate is null");
        this.columnReferences = ImmutableList.copyOf(checkNotNull(columnReferences, "columnReferences is null"));
    }

    @Override
    public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex)
    {
        ImmutableMap.Builder<C, Domain> domains = ImmutableMap.builder();

        for (ColumnReference<C> columnReference : columnReferences) {
            ColumnStatistics columnStatistics = statisticsByColumnIndex.get(columnReference.getOrdinal());

            Domain domain;
            if (columnStatistics == null) {
                // no stats for column
                domain = Domain.all(fixNonComparableType(Primitives.wrap(columnReference.getType().getJavaType())));
            }
            else {
                domain = getDomain(columnReference.getType(), numberOfRows, columnStatistics);
            }
            domains.put(columnReference.getColumn(), domain);
        }
        TupleDomain<C> stripeDomain = TupleDomain.withColumnDomains(domains.build());

        return effectivePredicate.overlaps(stripeDomain);
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, long rowCount, ColumnStatistics columnStatistics)
    {
        Class<?> boxedJavaType = Primitives.wrap(type.getJavaType());
        if (rowCount == 0) {
            return Domain.none(fixNonComparableType(boxedJavaType));
        }

        if (columnStatistics == null) {
            return Domain.all(fixNonComparableType(boxedJavaType));
        }

        if (columnStatistics.hasNumberOfValues() && columnStatistics.getNumberOfValues() == 0) {
            return Domain.onlyNull(fixNonComparableType(boxedJavaType));
        }

        boolean hasNullValue = columnStatistics.getNumberOfValues() != rowCount;

        if (boxedJavaType == Boolean.class && columnStatistics.getBooleanStatistics() != null) {
            BooleanStatistics booleanStatistics = columnStatistics.getBooleanStatistics();

            boolean hasTrueValues = (booleanStatistics.getTrueValueCount() != 0);
            boolean hasFalseValues = (columnStatistics.getNumberOfValues() != booleanStatistics.getTrueValueCount());
            if (hasTrueValues && hasFalseValues) {
                return Domain.all(Boolean.class);
            }
            if (hasTrueValues) {
                return Domain.create(SortedRangeSet.singleValue(true), hasNullValue);
            }
            if (hasFalseValues) {
                return Domain.create(SortedRangeSet.singleValue(false), hasNullValue);
            }
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.DATE) && columnStatistics.getDateStatistics() != null) {
            return createDomain(boxedJavaType, hasNullValue, columnStatistics.getDateStatistics(), value -> (long) value);
        }
        else if (boxedJavaType == Long.class && columnStatistics.getIntegerStatistics() != null) {
            return createDomain(boxedJavaType, hasNullValue, columnStatistics.getIntegerStatistics());
        }
        else if (boxedJavaType == Double.class && columnStatistics.getDoubleStatistics() != null) {
            return createDomain(boxedJavaType, hasNullValue, columnStatistics.getDoubleStatistics());
        }
        else if (boxedJavaType == Slice.class && columnStatistics.getStringStatistics() != null) {
            return createDomain(boxedJavaType, hasNullValue, columnStatistics.getStringStatistics());
        }
        return Domain.create(SortedRangeSet.all(fixNonComparableType(boxedJavaType)), hasNullValue);
    }

    private static Class<?> fixNonComparableType(Class<?> clazz)
    {
        // !!! HACK ALERT !!!
        // This is needed because SortedRangeSet.all/none requires that the argument type be self-comparable. See Marker#verifySelfComparable
        // However, the SortedRangeSet works fine when the only value involved are lowerUnbound and upperUnbound.
        // This hack shall be removed once TupleDomain is updated to support
        // The same hack can also be found in DomainTranslator
        if (clazz == Block.class) {
            return BogusComparable.class;
        }
        return clazz;
    }

    private static class BogusComparable
            implements Comparable<BogusComparable>
    {
        @Override
        public int compareTo(BogusComparable o)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static <T extends Comparable<T>> Domain createDomain(Class<?> boxedJavaType, boolean hasNullValue, RangeStatistics<T> rangeStatistics)
    {
        return createDomain(boxedJavaType, hasNullValue, rangeStatistics, value -> value);
    }

    private static <F, T extends Comparable<T>> Domain createDomain(Class<?> boxedJavaType, boolean hasNullValue, RangeStatistics<F> rangeStatistics, Function<F, T> function)
    {
        F min = rangeStatistics.getMin();
        F max = rangeStatistics.getMax();

        if (min != null && max != null) {
            return Domain.create(SortedRangeSet.of(Range.range(function.apply(min), true, function.apply(max), true)), hasNullValue);
        }
        if (max != null) {
            return Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(function.apply(max))), hasNullValue);
        }
        if (min != null) {
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

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("column", column)
                    .add("ordinal", ordinal)
                    .add("type", type)
                    .toString();
        }
    }
}
