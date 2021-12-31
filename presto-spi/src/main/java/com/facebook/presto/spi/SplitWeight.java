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
import com.fasterxml.jackson.annotation.JsonValue;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.function.Function;

import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;

public final class SplitWeight
{
    private static final long UNIT_VALUE = 100;
    private static final int UNIT_SCALE = 2; // Decimal scale such that (10 ^ UNIT_SCALE) == UNIT_VALUE
    private static final SplitWeight STANDARD_WEIGHT = new SplitWeight(UNIT_VALUE);

    private final long value;

    private SplitWeight(long value)
    {
        if (value <= 0) {
            throw new IllegalArgumentException("value must be > 0, found: " + value);
        }
        this.value = value;
    }

    /**
     * @return The internal integer representation for this weight value
     */
    @JsonValue
    public long getRawValue()
    {
        return value;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof SplitWeight)) {
            return false;
        }
        return this.value == ((SplitWeight) other).value;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(value);
    }

    @Override
    public String toString()
    {
        if (value == UNIT_VALUE) {
            return "1";
        }
        return BigDecimal.valueOf(value, -UNIT_SCALE).stripTrailingZeros().toPlainString();
    }

    @JsonCreator
    public static SplitWeight fromRawValue(long value)
    {
        return value == UNIT_VALUE ? STANDARD_WEIGHT : new SplitWeight(value);
    }

    public static SplitWeight standard()
    {
        return STANDARD_WEIGHT;
    }

    public static SplitWeight fromProportion(double weight)
    {
        if (weight <= 0 || !Double.isFinite(weight)) {
            throw new IllegalArgumentException("Invalid weight: " + weight);
        }
        // Must round up to avoid small weights rounding to 0
        return fromRawValue((long) Math.ceil(weight * UNIT_VALUE));
    }

    public static long rawValueForStandardSplitCount(int splitCount)
    {
        if (splitCount < 0) {
            throw new IllegalArgumentException("splitCount must be >= 0, found: " + splitCount);
        }
        return multiplyExact(splitCount, UNIT_VALUE);
    }

    public static <T> long rawValueSum(Collection<T> collection, Function<T, SplitWeight> getter)
    {
        long sum = 0;
        for (T item : collection) {
            long value = getter.apply(item).getRawValue();
            sum = addExact(sum, value);
        }
        return sum;
    }
}
