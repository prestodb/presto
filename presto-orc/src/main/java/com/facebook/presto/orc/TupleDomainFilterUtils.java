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

import com.facebook.presto.orc.TupleDomainFilter.BigintMultiRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintValuesUsingBitmask;
import com.facebook.presto.orc.TupleDomainFilter.BigintValuesUsingHashTable;
import com.facebook.presto.orc.TupleDomainFilter.BooleanValue;
import com.facebook.presto.orc.TupleDomainFilter.BytesRange;
import com.facebook.presto.orc.TupleDomainFilter.BytesValues;
import com.facebook.presto.orc.TupleDomainFilter.BytesValuesExclusive;
import com.facebook.presto.orc.TupleDomainFilter.DoubleRange;
import com.facebook.presto.orc.TupleDomainFilter.FloatRange;
import com.facebook.presto.orc.TupleDomainFilter.LongDecimalRange;
import com.facebook.presto.orc.TupleDomainFilter.MultiRange;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.orc.TupleDomainFilter.ALWAYS_FALSE;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public class TupleDomainFilterUtils
{
    private TupleDomainFilterUtils() {}

    public static TupleDomainFilter toFilter(Domain domain, int notInThreshold)
    {
        ValueSet values = domain.getValues();
        boolean nullAllowed = domain.isNullAllowed();

        if (values.isAll()) {
            checkArgument(!nullAllowed, "Unexpected allways-true filter");
            return IS_NOT_NULL;
        }

        if (values.isNone()) {
            checkArgument(nullAllowed, "Unexpected allways-false filter");
            return IS_NULL;
        }

        checkArgument(values instanceof SortedRangeSet, "Unexpected domain type: " + values.getClass().getSimpleName());

        List<Range> ranges = ((SortedRangeSet) values).getOrderedRanges();

        if (ranges.isEmpty() && nullAllowed) {
            return IS_NULL;
        }

        Type type = domain.getType();
        if (ranges.size() == 1) {
            return createRangeFilter(type, ranges.get(0), nullAllowed);
        }

        if (type == BOOLEAN) {
            return createBooleanFilter(ranges, nullAllowed);
        }

        List<TupleDomainFilter> rangeFilters = ranges.stream()
                .map(range -> createRangeFilter(type, range, false))
                .filter(not(ALWAYS_FALSE::equals))
                .collect(toImmutableList());
        if (rangeFilters.isEmpty()) {
            return nullAllowed ? IS_NULL : ALWAYS_FALSE;
        }

        TupleDomainFilter firstRangeFilter = rangeFilters.get(0);
        if (firstRangeFilter instanceof BigintRange) {
            List<BigintRange> bigintRanges = rangeFilters.stream()
                    .map(BigintRange.class::cast)
                    .collect(toImmutableList());

            if (bigintRanges.stream().allMatch(BigintRange::isSingleValue)) {
                return toBigintValues(
                        bigintRanges.stream()
                                .mapToLong(BigintRange::getLower)
                                .toArray(),
                        nullAllowed,
                        true);
            }

            Optional<List<Long>> exclusiveList = getExclusives(bigintRanges, notInThreshold);
            if (exclusiveList.isPresent()) {
                return toBigintValues(exclusiveList.get().stream().mapToLong(Long::longValue).toArray(), nullAllowed, false);
            }

            return BigintMultiRange.of(bigintRanges, nullAllowed);
        }

        if (firstRangeFilter instanceof BytesRange) {
            List<BytesRange> bytesRanges = rangeFilters.stream()
                    .map(BytesRange.class::cast)
                    .collect(toImmutableList());

            if (bytesRanges.stream().allMatch(BytesRange::isSingleValue)) {
                return BytesValues.of(
                        bytesRanges.stream()
                                .map(BytesRange::getLower)
                                .toArray(byte[][]::new),
                        nullAllowed);
            }

            if (isNotIn(ranges)) {
                return BytesValuesExclusive.of(
                        bytesRanges.stream()
                                .map(BytesRange::getLower)
                                .filter(Objects::nonNull)
                                .toArray(byte[][]::new),
                        nullAllowed);
            }
        }

        if (firstRangeFilter instanceof DoubleRange || firstRangeFilter instanceof FloatRange) {
            // != and NOT IN filters should return true when applied to NaN
            // E.g. NaN != 1.0 as well as NaN NOT IN (1.0, 2.5, 3.6) should return true; otherwise false.
            boolean nanAllowed = isNotIn(ranges);
            return MultiRange.of(rangeFilters, nullAllowed, nanAllowed);
        }

        return MultiRange.of(rangeFilters, nullAllowed, false);
    }

    /**
     * Returns the exclusive number list if it could use NOT IN filter optimization.
     *
     * We're not able to distinguish NOT IN statement from normal range compare
     * E.g.
     * NOT IN (9, 10, 25) would be [Long.MIN_VALUE, 8] [11, 24] [26, Long.MAX_VALUE]
     * (num < 8 OR (num > 11 AND number < 24) OR num > 26) could be the same
     *
     * When count of exclusive numbers is under the threshold, we can use NOT IN optimization
     * it would return exclusive number list; otherwise empty list.
     */
    private static Optional<List<Long>> getExclusives(List<BigintRange> bigintRanges, int notInThreshold)
    {
        if (bigintRanges.size() < 2) {
            return Optional.empty();
        }

        long count = getExclusiveCount(bigintRanges);
        if (count >= notInThreshold || count < 0) {
            return Optional.empty();
        }

        ImmutableList.Builder<Long> exclusiveNumbers = new ImmutableList.Builder<>();
        long start = Long.MIN_VALUE;
        for (int i = 0; i < bigintRanges.size(); i++) {
            if (start < bigintRanges.get(i).getLower()) {
                addExclusiveNumbers(exclusiveNumbers, start, bigintRanges.get(i).getLower() - 1);
            }

            if (bigintRanges.get(i).getUpper() == Long.MAX_VALUE) {
                start = Long.MAX_VALUE;
                break;
            }

            start = bigintRanges.get(i).getUpper() + 1;
        }
        if (start < Long.MAX_VALUE) {
            addExclusiveNumbers(exclusiveNumbers, start, Long.MAX_VALUE);
        }
        return Optional.of(exclusiveNumbers.build());
    }

    private static void addExclusiveNumbers(ImmutableList.Builder<Long> exclusiveNumbers, long start, long end)
    {
        while (start <= end) {
            exclusiveNumbers.add(start);
            start++;
        }
    }

    private static long getExclusiveCount(List<BigintRange> bigintRanges)
    {
        long count = 0;
        long start = Long.MIN_VALUE;
        for (int i = 0; i < bigintRanges.size(); i++) {
            if (start < bigintRanges.get(i).getLower()) {
                // Handle out of boundary cases
                if (bigintRanges.get(i).getLower() - start < 0) {
                    return -1;
                }
                count = count + (bigintRanges.get(i).getLower() - start);
                if (count < 0) {
                    return -1;
                }
            }

            if (bigintRanges.get(i).getUpper() == Long.MAX_VALUE) {
                start = Long.MAX_VALUE;
                break;
            }

            start = bigintRanges.get(i).getUpper() + 1;
        }
        if (start < Long.MAX_VALUE) {
            if (Long.MAX_VALUE - start < 0) {
                return -1;
            }
            count = count + (Long.MAX_VALUE - start) + 1;
            if (count < 0) {
                return -1;
            }
        }
        return count;
    }

    /**
     * Returns true is ranges represent != or NOT IN filter for double, float or string column.
     *
     * The logic is to return true if ranges are next to each other, but don't include the touch value.
     */
    private static boolean isNotIn(List<Range> ranges)
    {
        if (ranges.size() <= 1) {
            return false;
        }

        Range firstRange = ranges.get(0);
        Marker previousHigh = firstRange.getHigh();

        Type type = previousHigh.getType();
        if (type != DOUBLE && type != REAL && !isVarcharType(type) && !(type instanceof CharType)) {
            return false;
        }

        Range lastRange = ranges.get(ranges.size() - 1);
        if (!firstRange.getLow().isLowerUnbounded() || !lastRange.getHigh().isUpperUnbounded()) {
            return false;
        }

        for (int i = 1; i < ranges.size(); i++) {
            Range current = ranges.get(i);

            if (previousHigh.getBound() != Marker.Bound.BELOW ||
                    current.getLow().getBound() != Marker.Bound.ABOVE ||
                    type.compareTo(previousHigh.getValueBlock().get(), 0, current.getLow().getValueBlock().get(), 0) != 0) {
                return false;
            }

            previousHigh = current.getHigh();
        }

        return true;
    }

    private static TupleDomainFilter createBooleanFilter(List<Range> ranges, boolean nullAllowed)
    {
        boolean includesTrue = false;
        boolean includesFalse = false;
        for (Range range : ranges) {
            if (range.includes(Marker.exactly(BOOLEAN, true))) {
                includesTrue = true;
            }
            if (range.includes(Marker.exactly(BOOLEAN, false))) {
                includesFalse = true;
            }
        }

        if (includesTrue && includesFalse) {
            checkArgument(!nullAllowed, "Unexpected range of ALL values");
            return IS_NOT_NULL;
        }

        return BooleanValue.of(includesTrue, nullAllowed);
    }

    private static TupleDomainFilter createRangeFilter(Type type, Range range, boolean nullAllowed)
    {
        if (range.isAll()) {
            checkArgument(!nullAllowed, "Unexpected range of ALL values");
            return IS_NOT_NULL;
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT || type == TIMESTAMP || type == DATE) {
            return bigintRangeToFilter(range, nullAllowed);
        }
        if (type == BOOLEAN) {
            checkArgument(range.isSingleValue(), "Unexpected range of boolean values");
            return BooleanValue.of(((Boolean) range.getSingleValue()).booleanValue(), nullAllowed);
        }
        if (type == DOUBLE) {
            return doubleRangeToFilter(range, nullAllowed);
        }
        if (type == REAL) {
            return floatRangeToFilter(range, nullAllowed);
        }
        if (type instanceof DecimalType) {
            if (((DecimalType) type).isShort()) {
                return bigintRangeToFilter(range, nullAllowed);
            }
            return longDecimalRangeToFilter(range, nullAllowed);
        }
        if (isVarcharType(type) || type instanceof CharType) {
            return varcharRangeToFilter(range, nullAllowed);
        }

        throw new UnsupportedOperationException("Unsupported type: " + type.getDisplayName());
    }

    private static TupleDomainFilter bigintRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        long lowerLong = low.isLowerUnbounded() ? Long.MIN_VALUE : (long) low.getValue();
        long upperLong = high.isUpperUnbounded() ? Long.MAX_VALUE : (long) high.getValue();
        if (!high.isUpperUnbounded() && high.getBound() == Marker.Bound.BELOW) {
            --upperLong;
        }
        if (!low.isLowerUnbounded() && low.getBound() == Marker.Bound.ABOVE) {
            ++lowerLong;
        }
        if (upperLong < lowerLong) {
            return ALWAYS_FALSE;
        }
        return BigintRange.of(lowerLong, upperLong, nullAllowed);
    }

    private static TupleDomainFilter doubleRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        double lowerDouble = low.isLowerUnbounded() ? Double.MIN_VALUE : (double) low.getValue();
        double upperDouble = high.isUpperUnbounded() ? Double.MAX_VALUE : (double) high.getValue();
        if (!low.isLowerUnbounded() && Double.isNaN(lowerDouble)) {
            return ALWAYS_FALSE;
        }
        if (!high.isUpperUnbounded() && Double.isNaN(upperDouble)) {
            return ALWAYS_FALSE;
        }
        return DoubleRange.of(
                lowerDouble,
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                upperDouble,
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static TupleDomainFilter floatRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        float lowerFloat = low.isLowerUnbounded() ? Float.MIN_VALUE : intBitsToFloat(toIntExact((long) low.getValue()));
        float upperFloat = high.isUpperUnbounded() ? Float.MAX_VALUE : intBitsToFloat(toIntExact((long) high.getValue()));
        if (!low.isLowerUnbounded() && Float.isNaN(lowerFloat)) {
            return ALWAYS_FALSE;
        }
        if (!high.isUpperUnbounded() && Float.isNaN(upperFloat)) {
            return ALWAYS_FALSE;
        }
        return FloatRange.of(
                lowerFloat,
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                upperFloat,
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static TupleDomainFilter longDecimalRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        return LongDecimalRange.of(
                low.isLowerUnbounded() ? Long.MIN_VALUE : ((Slice) low.getValue()).getLong(0),
                low.isLowerUnbounded() ? Long.MIN_VALUE : ((Slice) low.getValue()).getLong(SIZE_OF_LONG),
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                high.isUpperUnbounded() ? Long.MAX_VALUE : ((Slice) high.getValue()).getLong(0),
                high.isUpperUnbounded() ? Long.MAX_VALUE : ((Slice) high.getValue()).getLong(SIZE_OF_LONG),
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static TupleDomainFilter varcharRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        Slice lowerValue = low.isLowerUnbounded() ? null : (Slice) low.getValue();
        Slice upperValue = high.isUpperUnbounded() ? null : (Slice) high.getValue();
        return BytesRange.of(lowerValue == null ? null : lowerValue.getBytes(),
                low.getBound() == Marker.Bound.ABOVE,
                upperValue == null ? null : upperValue.getBytes(),
                high.getBound() == Marker.Bound.BELOW, nullAllowed);
    }

    public static TupleDomainFilter toBigintValues(long[] values, boolean nullAllowed, boolean inclusive)
    {
        long min = values[0];
        long max = values[0];
        for (int i = 1; i < values.length; i++) {
            min = Math.min(min, values[i]);
            max = Math.max(max, values[i]);
        }

        // Filter based on a hash table uses up to 3 longs per value (the value itself + 1 or 2
        // slots in a hash table), e.g. up to 192 bits per value.
        // Filter based on a bitmap uses (max - min) / num-values bits per value.
        // Choose the filter that uses less bits per value.
        if ((max - min + 1) > Integer.MAX_VALUE || ((max - min + 1) / values.length) > 192) {
            return BigintValuesUsingHashTable.of(min, max, values, nullAllowed, inclusive);
        }

        return BigintValuesUsingBitmask.of(min, max, values, nullAllowed, inclusive);
    }
}
