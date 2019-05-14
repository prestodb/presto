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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public class TupleDomainFilters
{
    private TupleDomainFilters() {}

    public static Filter toFilter(Domain domain)
    {
        ValueSet values = domain.getValues();
        if (!(values instanceof SortedRangeSet)) {
            throw new UnsupportedOperationException("Unexpected domain type: " + values.getClass().getSimpleName());
        }

        List<Range> ranges = ((SortedRangeSet) values).getOrderedRanges();
        Type type = domain.getType();
        boolean nullAllowed = domain.isNullAllowed();

        Filter filter;
        if (ranges.isEmpty() && nullAllowed) {
            filter = Filters.isNull();
        }
        else if (ranges.size() == 1) {
            filter = createRangeFilter(type, ranges.get(0), nullAllowed);
        }
        else {
            List<Filter> rangeFilters = ranges.stream()
                    .map(r -> createRangeFilter(type, r, false))
                    .filter(f -> f != Filters.alwaysFalse())
                    .collect(toImmutableList());
            if (rangeFilters.isEmpty()) {
                filter = nullAllowed ? Filters.isNull() : Filters.alwaysFalse();
            }
            else {
                filter = Filters.createMultiRange(rangeFilters, nullAllowed);
            }
        }

        return filter;
    }

    private static Filter createRangeFilter(Type type, Range range, boolean nullAllowed)
    {
        if (range.isAll()) {
            return nullAllowed ? null : Filters.isNotNull();
        }
        if (isVarcharType(type) || type instanceof CharType) {
            return varcharRangeToFilter(range, nullAllowed);
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT || type == TIMESTAMP) {
            return bigintRangeToFilter(range, nullAllowed);
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
        if (type == BOOLEAN) {
            boolean booleanValue = ((Boolean) range.getSingleValue()).booleanValue();
            return range.isSingleValue() ? new Filters.BooleanValue(booleanValue, nullAllowed) : null;
        }

        throw new UnsupportedOperationException("Unsupported type: " + type.getDisplayName());
    }

    private static Filter bigintRangeToFilter(Range range, boolean nullAllowed)
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
            return Filters.alwaysFalse();
        }
        return new Filters.BigintRange(lowerLong, upperLong, nullAllowed);
    }

    private static Filter doubleRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        double lowerDouble = low.isLowerUnbounded() ? Double.MIN_VALUE : (double) low.getValue();
        double upperDouble = high.isUpperUnbounded() ? Double.MAX_VALUE : (double) high.getValue();
        return new Filters.DoubleRange(
                lowerDouble,
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                upperDouble,
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static Filter floatRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        float lowerFloat = low.isLowerUnbounded() ? Float.MIN_VALUE : intBitsToFloat(toIntExact((long) low.getValue()));
        float upperFloat = high.isUpperUnbounded() ? Float.MAX_VALUE : intBitsToFloat(toIntExact((long) high.getValue()));
        return new Filters.FloatRange(
                lowerFloat,
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                upperFloat,
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static Filter longDecimalRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        return new Filters.LongDecimalRange(
                low.isLowerUnbounded() ? 0 : ((Slice) low.getValue()).getLong(0),
                low.isLowerUnbounded() ? 0 : ((Slice) low.getValue()).getLong(SIZE_OF_LONG),
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                high.isLowerUnbounded() ? 0 : ((Slice) high.getValue()).getLong(0),
                high.isLowerUnbounded() ? 0 : ((Slice) high.getValue()).getLong(SIZE_OF_LONG),
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static Filter varcharRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        Marker.Bound lowerBound = low.getBound();
        Marker.Bound upperBound = high.getBound();
        Slice lowerValue = low.isLowerUnbounded() ? null : (Slice) low.getValue();
        Slice upperValue = high.isUpperUnbounded() ? null : (Slice) high.getValue();
        return new Filters.BytesRange(lowerValue == null ? null : lowerValue.getBytes(),
                lowerBound == Marker.Bound.EXACTLY,
                upperValue == null ? null : upperValue.getBytes(),
                upperBound == Marker.Bound.EXACTLY, nullAllowed);
    }
}
