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
import com.facebook.presto.orc.TupleDomainFilter.BigintValues;
import com.facebook.presto.orc.TupleDomainFilter.BooleanValue;
import com.facebook.presto.orc.TupleDomainFilter.BytesRange;
import com.facebook.presto.orc.TupleDomainFilter.BytesValues;
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
import io.airlift.slice.Slice;

import java.util.List;

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

    public static TupleDomainFilter toFilter(Domain domain)
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
                return BigintValues.of(
                        bigintRanges.stream()
                                .mapToLong(BigintRange::getLower)
                                .toArray(),
                        nullAllowed);
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
        }

        return MultiRange.of(rangeFilters, nullAllowed, isNanAllowed(ranges));
    }

    /**
     * Returns true is ranges represent != or NOT IN filter. These types of filters should
     * return true when applied to NaN.
     *
     * E.g. NaN != 1.0 as well as NaN NOT IN (1.0, 2.5, 3.6) should return true; otherwise false.
     *
     * The logic is to return true if ranges are next to each other, but don't include the touch value.
     */
    private static boolean isNanAllowed(List<Range> ranges)
    {
        if (ranges.size() <= 1) {
            return false;
        }

        Range firstRange = ranges.get(0);
        Marker previousHigh = firstRange.getHigh();

        Type type = previousHigh.getType();
        if (type != DOUBLE && type != REAL) {
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
}
