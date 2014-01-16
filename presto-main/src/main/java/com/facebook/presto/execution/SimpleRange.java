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

import com.facebook.presto.spi.Range;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public final class SimpleRange
{
    private final Optional<SimpleMarker> low;
    private final Optional<SimpleMarker> high;

    @JsonCreator
    public SimpleRange(
            @JsonProperty("low") Optional<SimpleMarker> low,
            @JsonProperty("high") Optional<SimpleMarker> high)
    {
        this.low = checkNotNull(low, "low is null");
        this.high = checkNotNull(high, "high is null");
    }

    @JsonProperty
    public Optional<SimpleMarker> getLow()
    {
        return low;
    }

    @JsonProperty
    public Optional<SimpleMarker> getHigh()
    {
        return high;
    }

    public static SimpleRange fromRange(Range range)
    {
        checkNotNull(range, "range is null");
        return new SimpleRange(
                Optional.fromNullable(SimpleMarker.fromMarker(range.getLow())),
                Optional.fromNullable(SimpleMarker.fromMarker(range.getHigh())));
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

        SimpleRange that = (SimpleRange) o;

        return Objects.equal(this.low, that.low) &&
                Objects.equal(this.high, that.high);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(low, high);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(low)
                .addValue(high)
                .toString();
    }
}
