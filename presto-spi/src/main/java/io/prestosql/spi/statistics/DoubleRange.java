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
package io.prestosql.spi.statistics;

import java.util.Objects;

import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DoubleRange
{
    private final double min;
    private final double max;

    public DoubleRange(double min, double max)
    {
        if (isNaN(min)) {
            throw new IllegalArgumentException("min must not be NaN");
        }
        if (isNaN(max)) {
            throw new IllegalArgumentException("max must not be NaN");
        }
        if (min > max) {
            throw new IllegalArgumentException(format("max must be greater than or equal to min. min: %s. max: %s. ", min, max));
        }
        this.min = min;
        this.max = max;
    }

    public double getMin()
    {
        return min;
    }

    public double getMax()
    {
        return max;
    }

    public static DoubleRange union(DoubleRange first, DoubleRange second)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        return new DoubleRange(min(first.min, second.min), max(first.max, second.max));
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
        DoubleRange range = (DoubleRange) o;
        return Double.compare(range.min, min) == 0 &&
                Double.compare(range.max, max) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(min, max);
    }

    @Override
    public String toString()
    {
        return "DoubleRange{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }
}
