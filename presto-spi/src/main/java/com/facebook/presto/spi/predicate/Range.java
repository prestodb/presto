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
package com.facebook.presto.spi.predicate;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A Range of values across the continuous space defined by the types of the Markers
 */
public final class Range
{
    private final Marker low;
    private final Marker high;

    @JsonCreator
    public Range(
            @JsonProperty("low") Marker low,
            @JsonProperty("high") Marker high)
    {
        requireNonNull(low, "value is null");
        requireNonNull(high, "value is null");
        if (!low.getType().equals(high.getType())) {
            throw new IllegalArgumentException(String.format("Marker types do not match: %s vs %s", low.getType(), high.getType()));
        }
        if (low.getBound() == Marker.Bound.BELOW) {
            throw new IllegalArgumentException("low bound must be EXACTLY or ABOVE");
        }
        if (high.getBound() == Marker.Bound.ABOVE) {
            throw new IllegalArgumentException("high bound must be EXACTLY or BELOW");
        }
        if (low.compareTo(high) > 0) {
            throw new IllegalArgumentException("low must be less than or equal to high");
        }
        this.low = low;
        this.high = high;
    }

    public static Range all(Type type)
    {
        return new Range(Marker.lowerUnbounded(type), Marker.upperUnbounded(type));
    }

    public static Range greaterThan(Type type, Object low)
    {
        return new Range(Marker.above(type, low), Marker.upperUnbounded(type));
    }

    public static Range greaterThanOrEqual(Type type, Object low)
    {
        return new Range(Marker.exactly(type, low), Marker.upperUnbounded(type));
    }

    public static Range lessThan(Type type, Object high)
    {
        return new Range(Marker.lowerUnbounded(type), Marker.below(type, high));
    }

    public static Range lessThanOrEqual(Type type, Object high)
    {
        return new Range(Marker.lowerUnbounded(type), Marker.exactly(type, high));
    }

    public static Range equal(Type type, Object value)
    {
        return new Range(Marker.exactly(type, value), Marker.exactly(type, value));
    }

    public static Range range(Type type, Object low, boolean lowInclusive, Object high, boolean highInclusive)
    {
        Marker lowMarker = lowInclusive ? Marker.exactly(type, low) : Marker.above(type, low);
        Marker highMarker = highInclusive ? Marker.exactly(type, high) : Marker.below(type, high);
        return new Range(lowMarker, highMarker);
    }

    public Type getType()
    {
        return low.getType();
    }

    @JsonProperty
    public Marker getLow()
    {
        return low;
    }

    @JsonProperty
    public Marker getHigh()
    {
        return high;
    }

    public boolean isSingleValue()
    {
        return low.getBound() == Marker.Bound.EXACTLY && low.equals(high);
    }

    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("Range does not have just a single value");
        }
        return low.getValue();
    }

    public boolean isAll()
    {
        return low.isLowerUnbounded() && high.isUpperUnbounded();
    }

    public boolean includes(Marker marker)
    {
        requireNonNull(marker, "marker is null");
        checkTypeCompatibility(marker);
        return low.compareTo(marker) <= 0 && high.compareTo(marker) >= 0;
    }

    public boolean contains(Range other)
    {
        checkTypeCompatibility(other);
        return this.getLow().compareTo(other.getLow()) <= 0 &&
                this.getHigh().compareTo(other.getHigh()) >= 0;
    }

    public Range span(Range other)
    {
        checkTypeCompatibility(other);
        Marker lowMarker = Marker.min(low, other.getLow());
        Marker highMarker = Marker.max(high, other.getHigh());
        return new Range(lowMarker, highMarker);
    }

    public boolean overlaps(Range other)
    {
        checkTypeCompatibility(other);
        return this.getLow().compareTo(other.getHigh()) <= 0 &&
                other.getLow().compareTo(this.getHigh()) <= 0;
    }

    public Range intersect(Range other)
    {
        checkTypeCompatibility(other);
        if (!this.overlaps(other)) {
            throw new IllegalArgumentException("Cannot intersect non-overlapping ranges");
        }
        Marker lowMarker = Marker.max(low, other.getLow());
        Marker highMarker = Marker.min(high, other.getHigh());
        return new Range(lowMarker, highMarker);
    }

    private void checkTypeCompatibility(Range range)
    {
        if (!getType().equals(range.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched Range types: %s vs %s", getType(), range.getType()));
        }
    }

    private void checkTypeCompatibility(Marker marker)
    {
        if (!getType().equals(marker.getType())) {
            throw new IllegalArgumentException(String.format("Marker of %s does not match Range of %s", marker.getType(), getType()));
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(low, high);
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
        final Range other = (Range) obj;
        return Objects.equals(this.low, other.low) &&
                Objects.equals(this.high, other.high);
    }

    public String toString(ConnectorSession session)
    {
        StringBuilder buffer = new StringBuilder();
        if (isSingleValue()) {
            buffer.append('[').append(low.getPrintableValue(session)).append(']');
        }
        else {
            buffer.append((low.getBound() == Marker.Bound.EXACTLY) ? '[' : '(');
            buffer.append(low.isLowerUnbounded() ? "<min>" : low.getPrintableValue(session));
            buffer.append(", ");
            buffer.append(high.isUpperUnbounded() ? "<max>" : high.getPrintableValue(session));
            buffer.append((high.getBound() == Marker.Bound.EXACTLY) ? ']' : ')');
        }
        return buffer.toString();
    }
}
