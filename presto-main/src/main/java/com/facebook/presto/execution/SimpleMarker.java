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
package com.facebook.presto.execution;

import com.facebook.presto.spi.Marker;
import com.facebook.presto.spi.SerializableNativeValue;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public final class SimpleMarker
{
    private final boolean inclusive;
    private final Comparable<?> value;
    private final Class<?> type;

    private SimpleMarker(boolean inclusive, Comparable<?> value, Class<?> type)
    {
        checkNotNull(value, "value is null");
        checkNotNull(type, "type is null");
        checkArgument(type.isInstance(value), String.format("value (%s) must be of specified type (%s)", value, type));
        this.inclusive = inclusive;
        this.value = value;
        this.type = type;
    }

    @JsonCreator
    public SimpleMarker(
            @JsonProperty("inclusive") boolean inclusive,
            @JsonProperty("value") SerializableNativeValue value)
    {
        this(inclusive, value.getValue(), value.getType());
    }

    @JsonProperty
    public boolean isInclusive()
    {
        return inclusive;
    }

    @JsonIgnore
    public Comparable<?> getValue()
    {
        return value;
    }

    @JsonProperty("value")
    public SerializableNativeValue getSerializableNativeValue()
    {
        return new SerializableNativeValue(type, value);
    }

    public static SimpleMarker fromMarker(Marker marker)
    {
        if (marker == null || marker.isUpperUnbounded() || marker.isLowerUnbounded()) {
            return null;
        }
        return new SimpleMarker(marker.getBound() == Marker.Bound.EXACTLY, marker.getValue(), marker.getType());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleMarker that = (SimpleMarker) o;

        return Objects.equals(this.inclusive, that.inclusive) &&
                Objects.equals(this.value, that.value) &&
                Objects.equals(this.type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(inclusive, value, type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(inclusive)
                .addValue(value)
                .addValue(type)
                .toString();
    }
}
