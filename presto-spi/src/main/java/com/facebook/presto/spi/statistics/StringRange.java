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
package com.facebook.presto.spi.statistics;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StringRange
{
    static final long STRING_RANGE_SIZE = ClassLayout.parseClass(StringRange.class).instanceSize();

    private final String min;
    private final String max;

    public StringRange(String min, String max)
    {
        this.min = requireNonNull(min, "min must not be null");
        this.max = requireNonNull(max, "max must not be null");
        if (min.compareTo(max) > 0) {
            throw new IllegalArgumentException(format("max must be greater than or equal to min. min: %s. max: %s", min, max));
        }
    }

    @JsonProperty
    public String getMin()
    {
        return min;
    }

    @JsonProperty
    public String getMax()
    {
        return max;
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
        StringRange range = (StringRange) o;
        return range.min.compareTo(min) == 0 &&
                range.max.compareTo(max) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(min, max);
    }

    @Override
    public String toString()
    {
        return "StringRange{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }
}
