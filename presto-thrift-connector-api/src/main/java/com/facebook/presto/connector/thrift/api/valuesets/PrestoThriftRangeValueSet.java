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
package com.facebook.presto.connector.thrift.api.valuesets;

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Marker.Bound;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.type.Type;
import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftEnum;
import io.airlift.drift.annotations.ThriftEnumValue;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.connector.thrift.api.PrestoThriftBlock.fromBlock;
import static com.facebook.presto.connector.thrift.api.valuesets.PrestoThriftRangeValueSet.PrestoThriftBound.fromBound;
import static com.facebook.presto.connector.thrift.api.valuesets.PrestoThriftRangeValueSet.PrestoThriftMarker.fromMarker;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static java.util.Objects.requireNonNull;

/**
 * A set containing zero or more Ranges of the same type over a continuous space of possible values.
 * Ranges are coalesced into the most compact representation of non-overlapping Ranges.
 * This structure is used with comparable and orderable types like bigint, integer, double, varchar, etc.
 */
@ThriftStruct
public final class PrestoThriftRangeValueSet
{
    private final List<PrestoThriftRange> ranges;

    @ThriftConstructor
    public PrestoThriftRangeValueSet(@ThriftField(name = "ranges") List<PrestoThriftRange> ranges)
    {
        this.ranges = requireNonNull(ranges, "ranges is null");
    }

    @ThriftField(1)
    public List<PrestoThriftRange> getRanges()
    {
        return ranges;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftRangeValueSet other = (PrestoThriftRangeValueSet) obj;
        return Objects.equals(this.ranges, other.ranges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ranges);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRanges", ranges.size())
                .toString();
    }

    public static PrestoThriftRangeValueSet fromSortedRangeSet(SortedRangeSet valueSet)
    {
        List<PrestoThriftRange> ranges = valueSet.getOrderedRanges().stream()
                .map(PrestoThriftRange::fromRange)
                .collect(toImmutableList());
        return new PrestoThriftRangeValueSet(ranges);
    }

    public static SortedRangeSet toSortedRangeSet(PrestoThriftRangeValueSet valueSet, Type type)
    {
        List<Range> ranges = valueSet.getRanges().stream()
                .map(range -> PrestoThriftRange.toRange(range, type))
                .collect(toImmutableList());
        return SortedRangeSet.copyOf(type, ranges);
    }

    @ThriftEnum
    public enum PrestoThriftBound
    {
        BELOW(1),   // lower than the value, but infinitesimally close to the value
        EXACTLY(2), // exactly the value
        ABOVE(3);   // higher than the value, but infinitesimally close to the value

        private final int value;

        PrestoThriftBound(int value)
        {
            this.value = value;
        }

        @ThriftEnumValue
        public int getValue()
        {
            return value;
        }

        public static PrestoThriftBound fromBound(Bound bound)
        {
            switch (bound) {
                case BELOW:
                    return BELOW;
                case EXACTLY:
                    return EXACTLY;
                case ABOVE:
                    return ABOVE;
                default:
                    throw new IllegalArgumentException("Unknown bound: " + bound);
            }
        }

        public static Bound toBound(PrestoThriftBound bound)
        {
            switch (bound) {
                case BELOW:
                    return Bound.BELOW;
                case EXACTLY:
                    return Bound.EXACTLY;
                case ABOVE:
                    return Bound.ABOVE;
                default:
                    throw new IllegalArgumentException("Unknown bound: " + bound);
            }
        }
    }

    /**
     * LOWER UNBOUNDED is specified with an empty value and an ABOVE bound
     * UPPER UNBOUNDED is specified with an empty value and a BELOW bound
     */
    @ThriftStruct
    public static final class PrestoThriftMarker
    {
        private final PrestoThriftBlock value;
        private final PrestoThriftBound bound;

        @ThriftConstructor
        public PrestoThriftMarker(@Nullable PrestoThriftBlock value, PrestoThriftBound bound)
        {
            checkArgument(value == null || value.numberOfRecords() == 1, "value must contain exactly one record when present");
            this.value = value;
            this.bound = requireNonNull(bound, "bound is null");
        }

        @Nullable
        @ThriftField(value = 1, requiredness = OPTIONAL)
        public PrestoThriftBlock getValue()
        {
            return value;
        }

        @ThriftField(2)
        public PrestoThriftBound getBound()
        {
            return bound;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PrestoThriftMarker other = (PrestoThriftMarker) obj;
            return Objects.equals(this.value, other.value) &&
                    Objects.equals(this.bound, other.bound);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value, bound);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("value", value)
                    .add("bound", bound)
                    .toString();
        }

        public static PrestoThriftMarker fromMarker(Marker marker)
        {
            PrestoThriftBlock value = marker.getValueBlock().isPresent() ? fromBlock(marker.getValueBlock().get(), marker.getType()) : null;
            return new PrestoThriftMarker(value, fromBound(marker.getBound()));
        }

        public static Marker toMarker(PrestoThriftMarker marker, Type type)
        {
            Optional<Block> value = (marker.getValue() != null) ? Optional.of(marker.getValue().toBlock(type)) : Optional.empty();
            return new Marker(type, value, PrestoThriftBound.toBound(marker.getBound()));
        }
    }

    @ThriftStruct
    public static final class PrestoThriftRange
    {
        private final PrestoThriftMarker low;
        private final PrestoThriftMarker high;

        @ThriftConstructor
        public PrestoThriftRange(PrestoThriftMarker low, PrestoThriftMarker high)
        {
            this.low = requireNonNull(low, "low is null");
            this.high = requireNonNull(high, "high is null");
        }

        @ThriftField(1)
        public PrestoThriftMarker getLow()
        {
            return low;
        }

        @ThriftField(2)
        public PrestoThriftMarker getHigh()
        {
            return high;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PrestoThriftRange other = (PrestoThriftRange) obj;
            return Objects.equals(this.low, other.low) &&
                    Objects.equals(this.high, other.high);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(low, high);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("low", low)
                    .add("high", high)
                    .toString();
        }

        public static PrestoThriftRange fromRange(Range range)
        {
            return new PrestoThriftRange(fromMarker(range.getLow()), fromMarker(range.getHigh()));
        }

        public static Range toRange(PrestoThriftRange range, Type type)
        {
            return new Range(PrestoThriftMarker.toMarker(range.getLow(), type), PrestoThriftMarker.toMarker(range.getHigh(), type));
        }
    }
}
