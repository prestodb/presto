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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;

public class DecimalStatistics
        implements RangeStatistics<BigDecimal>, Hashable
{
    // 1 byte to denote if null
    public static final long DECIMAL_VALUE_BYTES_OVERHEAD = Byte.BYTES;

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalStatistics.class).instanceSize();
    // BigDecimal contains BigInteger and BigInteger contains an integer array.
    // The size of the integer array is not accessible from outside; thus rely on callers to tell how large the size is.
    private static final long BIG_DECIMAL_INSTANCE_SIZE = ClassLayout.parseClass(BigDecimal.class).instanceSize() + ClassLayout.parseClass(BigInteger.class).instanceSize() + sizeOf(new int[0]);

    // TODO: replace min/max with LongDecimal/ShortDecimal to calculate retained size
    private final BigDecimal minimum;
    private final BigDecimal maximum;
    private final long retainedSizeInBytes;
    private final long decimalSizeInBytes;

    @SuppressWarnings("NumberEquality")
    public DecimalStatistics(BigDecimal minimum, BigDecimal maximum, long decimalSizeInBytes)
    {
        checkArgument(minimum == null || maximum == null || minimum.compareTo(maximum) <= 0, "minimum is not less than maximum");
        this.minimum = minimum;
        this.maximum = maximum;

        long retainedSizeInBytes = 0;
        if (minimum != null) {
            retainedSizeInBytes += BIG_DECIMAL_INSTANCE_SIZE + decimalSizeInBytes;
        }
        if (maximum != null && minimum != maximum) {
            retainedSizeInBytes += BIG_DECIMAL_INSTANCE_SIZE + decimalSizeInBytes;
        }
        this.retainedSizeInBytes = retainedSizeInBytes + INSTANCE_SIZE;
        this.decimalSizeInBytes = decimalSizeInBytes;
    }

    @Override
    public BigDecimal getMin()
    {
        return minimum;
    }

    @Override
    public BigDecimal getMax()
    {
        return maximum;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    public long getDecimalSizeInBytes()
    {
        return decimalSizeInBytes;
    }

    public long getMinAverageValueSizeInBytes()
    {
        return DECIMAL_VALUE_BYTES_OVERHEAD + decimalSizeInBytes;
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
        DecimalStatistics that = (DecimalStatistics) o;
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
                .add("minimum", minimum)
                .add("maximum", maximum)
                .toString();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        hasher.putOptionalBigDecimal(minimum)
                .putOptionalBigDecimal(maximum);
    }
}
