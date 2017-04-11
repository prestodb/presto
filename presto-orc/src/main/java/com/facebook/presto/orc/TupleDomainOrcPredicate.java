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

import com.facebook.presto.orc.metadata.statistics.BooleanStatistics;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.RangeStatistics;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hive.common.util.BloomFilter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Chars.trimSpacesAndTruncateToLength;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class TupleDomainOrcPredicate<C>
        implements OrcPredicate
{
    private final TupleDomain<C> effectivePredicate;
    private final List<ColumnReference<C>> columnReferences;

    private final boolean orcBloomFiltersEnabled;

    public TupleDomainOrcPredicate(TupleDomain<C> effectivePredicate, List<ColumnReference<C>> columnReferences, boolean orcBloomFiltersEnabled)
    {
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.columnReferences = ImmutableList.copyOf(requireNonNull(columnReferences, "columnReferences is null"));
        this.orcBloomFiltersEnabled = orcBloomFiltersEnabled;
    }

    @Override
    public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex)
    {
        Optional<Map<C, Domain>> optionalEffectivePredicateDomains = effectivePredicate.getDomains();
        if (!optionalEffectivePredicateDomains.isPresent()) {
            // effective predicate is none, so skip this section
            return false;
        }
        Map<C, Domain> effectivePredicateDomains = optionalEffectivePredicateDomains.get();

        for (ColumnReference<C> columnReference : columnReferences) {
            Domain predicateDomain = effectivePredicateDomains.get(columnReference.getColumn());
            if (predicateDomain == null) {
                // no predicate on this column, so we can't exclude this section
                continue;
            }
            ColumnStatistics columnStatistics = statisticsByColumnIndex.get(columnReference.getOrdinal());
            if (columnStatistics == null) {
                // no statistics for this column, so we can't exclude this section
                continue;
            }

            if (!columnOverlaps(columnReference, predicateDomain, numberOfRows, columnStatistics)) {
                return false;
            }
        }

        // this section was not excluded
        return true;
    }

    private boolean columnOverlaps(ColumnReference<C> columnReference, Domain predicateDomain, long numberOfRows, ColumnStatistics columnStatistics)
    {
        Domain stripeDomain = getDomain(columnReference.getType(), numberOfRows, columnStatistics);
        if (!stripeDomain.overlaps(predicateDomain)) {
            // there is no overlap between the predicate and this column
            return false;
        }

        // if bloom filters are not enabled, we can not restrict the range overlap
        if (!orcBloomFiltersEnabled) {
            return true;
        }

        // if there an overlap in null values, the bloom filter can not eliminate the overlap
        if (predicateDomain.isNullAllowed() && stripeDomain.isNullAllowed()) {
            return true;
        }

        // extract the discrete values from the predicate
        Optional<Collection<Object>> discreteValues = extractDiscreteValues(predicateDomain.getValues());
        if (!discreteValues.isPresent()) {
            // values are not discrete, so we can't exclude this section
            return true;
        }

        HiveBloomFilter bloomFilter = columnStatistics.getBloomFilter();
        if (bloomFilter == null) {
            // no bloom filter so we can't exclude this section
            return true;
        }

        // if none of the discrete predicate values are found in the bloom filter, there is no overlap and the section should be skipped
        if (discreteValues.get().stream().noneMatch(value -> checkInBloomFilter(bloomFilter, value, stripeDomain.getType()))) {
            return false;
        }
        return true;
    }

    @VisibleForTesting
    public static Optional<Collection<Object>> extractDiscreteValues(ValueSet valueSet)
    {
        return valueSet.getValuesProcessor().transform(
                ranges -> {
                    ImmutableList.Builder<Object> discreteValues = ImmutableList.builder();
                    for (Range range : ranges.getOrderedRanges()) {
                        if (!range.isSingleValue()) {
                            return Optional.empty();
                        }
                        discreteValues.add(range.getSingleValue());
                    }
                    return Optional.of(discreteValues.build());
                },
                discreteValues -> Optional.of(discreteValues.getValues()),
                allOrNone -> allOrNone.isAll() ? Optional.empty() : Optional.of(ImmutableList.of()));
    }

    // checks whether a value part of the effective predicate is likely to be part of this bloom filter
    @VisibleForTesting
    public static boolean checkInBloomFilter(BloomFilter bloomFilter, Object predicateValue, Type sqlType)
    {
        if (sqlType == TINYINT || sqlType == SMALLINT || sqlType == INTEGER || sqlType == BIGINT) {
            return bloomFilter.testLong(((Number) predicateValue).longValue());
        }

        if (sqlType == DOUBLE) {
            return bloomFilter.testDouble((Double) predicateValue);
        }

        if (sqlType instanceof VarcharType || sqlType instanceof VarbinaryType) {
            return bloomFilter.test(((Slice) predicateValue).getBytes());
        }

        // todo support DECIMAL, FLOAT, DATE, TIMESTAMP, and CHAR
        return true;
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, long rowCount, ColumnStatistics columnStatistics)
    {
        if (rowCount == 0) {
            return Domain.none(type);
        }

        if (columnStatistics == null) {
            return Domain.all(type);
        }

        if (columnStatistics.hasNumberOfValues() && columnStatistics.getNumberOfValues() == 0) {
            return Domain.onlyNull(type);
        }

        boolean hasNullValue = columnStatistics.getNumberOfValues() != rowCount;

        if (type.getJavaType() == boolean.class && columnStatistics.getBooleanStatistics() != null) {
            BooleanStatistics booleanStatistics = columnStatistics.getBooleanStatistics();

            boolean hasTrueValues = (booleanStatistics.getTrueValueCount() != 0);
            boolean hasFalseValues = (columnStatistics.getNumberOfValues() != booleanStatistics.getTrueValueCount());
            if (hasTrueValues && hasFalseValues) {
                return Domain.all(BOOLEAN);
            }
            if (hasTrueValues) {
                return Domain.create(ValueSet.of(BOOLEAN, true), hasNullValue);
            }
            if (hasFalseValues) {
                return Domain.create(ValueSet.of(BOOLEAN, false), hasNullValue);
            }
        }
        else if (isShortDecimal(type) && columnStatistics.getDecimalStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDecimalStatistics(), value -> rescale(value, (DecimalType) type).unscaledValue().longValue());
        }
        else if (isLongDecimal(type) && columnStatistics.getDecimalStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDecimalStatistics(), value -> encodeUnscaledValue(rescale(value, (DecimalType) type).unscaledValue()));
        }
        else if (isCharType(type) && columnStatistics.getStringStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getStringStatistics(), value -> trimSpacesAndTruncateToLength(value, type));
        }
        else if (isVarcharType(type) && columnStatistics.getStringStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getStringStatistics());
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.DATE) && columnStatistics.getDateStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDateStatistics(), value -> (long) value);
        }
        else if (type.getJavaType() == long.class && columnStatistics.getIntegerStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getIntegerStatistics());
        }
        else if (type.getJavaType() == double.class && columnStatistics.getDoubleStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDoubleStatistics());
        }
        else if (REAL.equals(type) && columnStatistics.getDoubleStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDoubleStatistics(), value -> (long) floatToRawIntBits(value.floatValue()));
        }
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    private static <T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, RangeStatistics<T> rangeStatistics)
    {
        return createDomain(type, hasNullValue, rangeStatistics, value -> value);
    }

    private static <F, T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, RangeStatistics<F> rangeStatistics, Function<F, T> function)
    {
        F min = rangeStatistics.getMin();
        F max = rangeStatistics.getMax();

        if (min != null && max != null) {
            return Domain.create(ValueSet.ofRanges(Range.range(type, function.apply(min), true, function.apply(max), true)), hasNullValue);
        }
        if (max != null) {
            return Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, function.apply(max))), hasNullValue);
        }
        if (min != null) {
            return Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, function.apply(min))), hasNullValue);
        }
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    public static class ColumnReference<C>
    {
        private final C column;
        private final int ordinal;
        private final Type type;

        public ColumnReference(C column, int ordinal, Type type)
        {
            this.column = requireNonNull(column, "column is null");
            checkArgument(ordinal >= 0, "ordinal is negative");
            this.ordinal = ordinal;
            this.type = requireNonNull(type, "type is null");
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
            return toStringHelper(this)
                    .add("column", column)
                    .add("ordinal", ordinal)
                    .add("type", type)
                    .toString();
        }
    }
}
