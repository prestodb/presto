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
import io.airlift.slice.Slice;
import jakarta.annotation.Nullable;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class StringStatistics
        implements RangeStatistics<Slice>, Hashable
{
    // 1 byte to denote if null + 4 bytes to denote offset
    public static final long STRING_VALUE_BYTES_OVERHEAD = Byte.BYTES + Integer.BYTES;

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StringStatistics.class).instanceSize();

    @Nullable
    private final Slice minimum;
    @Nullable
    private final Slice maximum;
    @Nullable
    private final boolean lowerBoundSet;
    @Nullable
    private final boolean upperBoundSet;
    private final long sum;

    public StringStatistics(@Nullable Slice minimum, @Nullable Slice maximum, boolean lowerBoundSet, boolean upperBoundSet, long sum)
    {
        checkArgument(minimum == null || maximum == null || minimum.compareTo(maximum) <= 0, "minimum is not less than maximum");
        this.minimum = minimum;
        this.maximum = maximum;
        this.lowerBoundSet = lowerBoundSet;
        this.upperBoundSet = upperBoundSet;
        this.sum = sum;
    }

    @Override
    public Slice getMin()
    {
        return minimum;
    }

    @Override
    public Slice getMax()
    {
        return maximum;
    }

    public boolean isLowerBoundSet()
    {
        return lowerBoundSet;
    }

    public boolean isUpperBoundSet()
    {
        return upperBoundSet;
    }

    public long getSum()
    {
        return sum;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + (minimum == null ? 0 : minimum.getRetainedSize()) + ((maximum == null || maximum == minimum) ? 0 : maximum.getRetainedSize());
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
        StringStatistics that = (StringStatistics) o;
        return Objects.equals(minimum, that.minimum) &&
                Objects.equals(maximum, that.maximum) &&
                Objects.equals(lowerBoundSet, that.lowerBoundSet) &&
                Objects.equals(upperBoundSet, that.upperBoundSet) &&
                Objects.equals(sum, that.sum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minimum, maximum, lowerBoundSet, upperBoundSet, sum);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("min", minimum == null ? "<null>" : minimum.toStringUtf8())
                .add("max", maximum == null ? "<null>" : maximum.toStringUtf8())
                .add("lowerBound", lowerBoundSet)
                .add("upperBound", upperBoundSet)
                .add("sum", sum)
                .toString();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        hasher.putOptionalSlice(minimum)
                .putOptionalSlice(maximum)
                .putBoolean(lowerBoundSet)
                .putBoolean(upperBoundSet)
                .putLong(sum);
    }
}
