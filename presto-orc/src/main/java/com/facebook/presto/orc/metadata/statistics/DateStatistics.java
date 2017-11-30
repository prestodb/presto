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
package com.facebook.presto.orc.metadata.statistics;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class DateStatistics
        implements RangeStatistics<Integer>
{
    // 1 byte to denote if null + 4 bytes for the value (date is of integer type)
    public static final long DATE_VALUE_BYTES = Byte.BYTES + Integer.BYTES;

    private final Integer minimum;
    private final Integer maximum;

    public DateStatistics(Integer minimum, Integer maximum)
    {
        checkArgument(minimum == null || maximum == null || minimum <= maximum, "minimum is not less than maximum");
        this.minimum = minimum;
        this.maximum = maximum;
    }

    @Override
    public Integer getMin()
    {
        return minimum;
    }

    @Override
    public Integer getMax()
    {
        return maximum;
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
        DateStatistics that = (DateStatistics) o;
        return Objects.equals(minimum, that.minimum) &&
                Objects.equals(maximum, that.maximum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minimum, maximum);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("min", minimum)
                .add("max", maximum)
                .toString();
    }
}
