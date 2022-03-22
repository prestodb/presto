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

import com.google.common.base.MoreObjects.ToStringHelper;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static com.facebook.presto.orc.metadata.statistics.IntegerStatistics.INTEGER_VALUE_BYTES;
import static java.util.Objects.requireNonNull;

public class IntegerColumnStatistics
        extends ColumnStatistics
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IntegerColumnStatistics.class).instanceSize();

    private final IntegerStatistics integerStatistics;

    public IntegerColumnStatistics(
            Long numberOfValues,
            HiveBloomFilter bloomFilter,
            IntegerStatistics integerStatistics)
    {
        super(numberOfValues, bloomFilter);
        requireNonNull(integerStatistics, "integerStatistics is null");
        this.integerStatistics = integerStatistics;
    }

    @Override
    public IntegerStatistics getIntegerStatistics()
    {
        return integerStatistics;
    }

    @Override
    public long getMinAverageValueSizeInBytes()
    {
        return INTEGER_VALUE_BYTES;
    }

    @Override
    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return new IntegerColumnStatistics(
                getNumberOfValues(),
                bloomFilter,
                integerStatistics);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long sizeInBytes = INSTANCE_SIZE + getMembersSizeInBytes();
        return sizeInBytes + integerStatistics.getRetainedSizeInBytes();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        super.addHash(hasher);
        hasher.putOptionalHashable(integerStatistics);
    }

    @Override
    protected ToStringHelper getToStringHelper()
    {
        return super.getToStringHelper()
                .add("integerStatistics", integerStatistics);
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
        IntegerColumnStatistics that = (IntegerColumnStatistics) o;
        return equalsInternal(that) && Objects.equals(integerStatistics, that.integerStatistics);
    }

    public int hashCode()
    {
        return Objects.hash(super.hashCode(), integerStatistics);
    }
}
