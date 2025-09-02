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

import com.facebook.presto.orc.metadata.statistics.StatisticsHasher.Hashable;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class DateStatistics
        implements RangeStatistics<Integer>, Hashable
{
    // 1 byte to denote if null + 4 bytes for the value (date is of integer type)
    public static final long DATE_VALUE_BYTES = Byte.BYTES + Integer.BYTES;

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DateStatistics.class).instanceSize();

    private final boolean hasMinimum;
    private final boolean hasMaximum;

    private final int minimum;
    private final int maximum;

    public DateStatistics(Integer minimum, Integer maximum)
    {
        checkArgument(minimum == null || maximum == null || minimum <= maximum, "minimum is not less than maximum");

        this.hasMinimum = minimum != null;
        this.minimum = hasMinimum ? minimum : 0;

        this.hasMaximum = maximum != null;
        this.maximum = hasMaximum ? maximum : 0;
    }

    @Override
    public Integer getMin()
    {
        return hasMinimum ? minimum : null;
    }

    @Override
    public Integer getMax()
    {
        return hasMaximum ? maximum : null;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
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
        return Objects.equals(getMin(), that.getMin()) &&
                Objects.equals(getMax(), that.getMax());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getMin(), getMax());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("min", getMin())
                .add("max", getMax())
                .toString();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        hasher.putOptionalInt(hasMinimum, minimum)
                .putOptionalInt(hasMaximum, maximum);
    }
}
