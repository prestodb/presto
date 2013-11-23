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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

/**
 * A point on the continuous space defined by the specified type.
 * Each point may be just below, exact, or just above the specified value according to the Bound.
 */
public final class Marker
        implements Comparable<Marker>
{
    public static enum Bound
    {
        BELOW,   // lower than the value, but infinitesimally close to the value
        EXACTLY, // exactly the value
        ABOVE    // higher than the value, but infinitesimally close to the value
    }

    private final Class<?> type;
    private final Comparable<?> value;
    private final Bound bound;

    /**
     * LOWER UNBOUNDED is specified with a null value and a ABOVE bound
     * UPPER UNBOUNDED is specified with a null value and a BELOW bound
     */
    private Marker(Class<?> type, Comparable<?> value, Bound bound)
    {
        Objects.requireNonNull(type, "type is null");
        Objects.requireNonNull(bound, "bound is null");
        if (!verifySelfComparable(type)) {
            throw new IllegalArgumentException("type must be comparable to itself: " + type);
        }
        if (value == null && bound == Bound.EXACTLY) {
            throw new IllegalArgumentException("Can not be equal to unbounded");
        }
        if (value != null && !type.isInstance(value)) {
            throw new IllegalArgumentException(String.format("value (%s) must be of specified type (%s)", value, type));
        }
        this.type = type;
        this.value = value;
        this.bound = bound;
    }

    @JsonCreator
    public Marker(
            @JsonProperty("value") SerializableNativeValue value,
            @JsonProperty("bound") Bound bound)
    {
        this(value.getType(), value.getValue(), bound);
    }

    private static boolean verifySelfComparable(Class<?> type)
    {
        // TODO: expand this with the proper implementation
        for (Type interfaceType : type.getGenericInterfaces()) {
            if (interfaceType instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) interfaceType;
                if (parameterizedType.getRawType().equals(Comparable.class)) {
                    Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                    Type typeArgument = actualTypeArguments[0];
                    if (typeArgument.equals(type)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static Marker upperUnbounded(Class<?> type)
    {
        return new Marker(type, null, Bound.BELOW);
    }

    public static Marker lowerUnbounded(Class<?> type)
    {
        return new Marker(type, null, Bound.ABOVE);
    }

    public static Marker above(Comparable<?> value)
    {
        Objects.requireNonNull(value, "value is null");
        return new Marker(value.getClass(), value, Bound.ABOVE);
    }

    public static Marker exactly(Comparable<?> value)
    {
        Objects.requireNonNull(value, "value is null");
        return new Marker(value.getClass(), value, Bound.EXACTLY);
    }

    public static Marker below(Comparable<?> value)
    {
        Objects.requireNonNull(value, "value is null");
        return new Marker(value.getClass(), value, Bound.BELOW);
    }

    @JsonIgnore
    public Class<?> getType()
    {
        return type;
    }

    @JsonIgnore
    public Comparable<?> getValue()
    {
        if (value == null) {
            throw new IllegalStateException("Can not get value for unbounded");
        }
        return value;
    }

    @JsonProperty("value")
    public SerializableNativeValue getSerializableNativeValue()
    {
        return new SerializableNativeValue(type, value);
    }

    @JsonProperty
    public Bound getBound()
    {
        return bound;
    }

    @JsonIgnore
    public boolean isUpperUnbounded()
    {
        return value == null && bound == Bound.BELOW;
    }

    @JsonIgnore
    public boolean isLowerUnbounded()
    {
        return value == null && bound == Bound.ABOVE;
    }

    private void checkTypeCompatibility(Marker marker)
    {
        if (!type.equals(marker.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched Marker types: %s vs %s", type, marker.getType()));
        }
    }

    /**
     * Adjacency is defined by two Markers being infinitesimally close to each other.
     * This means they must share the same value and have adjacent Bounds.
     */
    public boolean isAdjacent(Marker other)
    {
        checkTypeCompatibility(other);
        if (isUpperUnbounded() || isLowerUnbounded() || other.isUpperUnbounded() || other.isLowerUnbounded()) {
            return false;
        }
        if (compare(value, other.value) != 0) {
            return false;
        }
        return (bound == Bound.EXACTLY && other.bound != Bound.EXACTLY) ||
               (bound != Bound.EXACTLY && other.bound == Bound.EXACTLY);
    }

    public Marker greaterAdjacent()
    {
        if (value == null) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                return new Marker(type, value, Bound.EXACTLY);
            case EXACTLY:
                return new Marker(type, value, Bound.ABOVE);
            case ABOVE:
                throw new IllegalStateException("No greater marker adjacent to an ABOVE bound");
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    public Marker lesserAdjacent()
    {
        if (value == null) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                throw new IllegalStateException("No lesser marker adjacent to a BELOW bound");
            case EXACTLY:
                return new Marker(type, value, Bound.BELOW);
            case ABOVE:
                return new Marker(type, value, Bound.EXACTLY);
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    @Override
    public int compareTo(Marker o)
    {
        checkTypeCompatibility(o);
        if (isUpperUnbounded()) {
            return o.isUpperUnbounded() ? 0 : 1;
        }
        if (isLowerUnbounded()) {
            return o.isLowerUnbounded() ? 0 : -1;
        }
        if (o.isUpperUnbounded()) {
            return -1;
        }
        if (o.isLowerUnbounded()) {
            return 1;
        }
        // INVARIANT: value and o.value not null

        int compare = compare(value, o.value);
        if (compare == 0) {
            if (bound == o.bound) {
                return 0;
            }
            if (bound == Bound.BELOW) {
                return -1;
            }
            if (bound == Bound.ABOVE) {
                return 1;
            }
            // INVARIANT: bound == EXACTLY
            return (o.bound == Bound.BELOW) ? 1 : -1;
        }
        return compare;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static int compare(Comparable<?> value1, Comparable<?> value2)
    {
        // This is terrible, but it should be safe as we have checked the compatibility in the constructor
        return ((Comparable) value1).compareTo(value2);
    }

    public static Marker min(Marker marker1, Marker marker2)
    {
        return marker1.compareTo(marker2) <= 0 ? marker1 : marker2;
    }

    public static Marker max(Marker marker1, Marker marker2)
    {
        return marker1.compareTo(marker2) >= 0 ? marker1 : marker2;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, value, bound);
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
        final Marker other = (Marker) obj;
        return Objects.equals(this.type, other.type) && Objects.equals(this.value, other.value) && Objects.equals(this.bound, other.bound);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Marker{");
        sb.append("type=").append(type);
        sb.append(", value=").append(value);
        sb.append(", bound=").append(bound);
        sb.append('}');
        return sb.toString();
    }
}
