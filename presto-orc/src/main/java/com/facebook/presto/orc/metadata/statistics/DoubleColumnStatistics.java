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

import static com.facebook.presto.orc.metadata.statistics.DoubleStatistics.DOUBLE_VALUE_BYTES;
import static java.util.Objects.requireNonNull;

public class DoubleColumnStatistics
        extends ColumnStatistics
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DoubleColumnStatistics.class).instanceSize();

    private final DoubleStatistics doubleStatistics;

    public DoubleColumnStatistics(
            Long numberOfValues,
            HiveBloomFilter bloomFilter,
            DoubleStatistics doubleStatistics)
    {
        super(numberOfValues, bloomFilter);
        requireNonNull(doubleStatistics, "doubleStatistics is null");
        this.doubleStatistics = doubleStatistics;
    }

    @Override
    public DoubleStatistics getDoubleStatistics()
    {
        return doubleStatistics;
    }

    @Override
    public long getMinAverageValueSizeInBytes()
    {
        return DOUBLE_VALUE_BYTES;
    }

    @Override
    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return new DoubleColumnStatistics(
                getNumberOfValues(),
                bloomFilter,
                doubleStatistics);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long sizeInBytes = INSTANCE_SIZE + getMembersSizeInBytes();
        return sizeInBytes + doubleStatistics.getRetainedSizeInBytes();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        super.addHash(hasher);
        hasher.putOptionalHashable(doubleStatistics);
    }

    @Override
    protected ToStringHelper getToStringHelper()
    {
        return super.getToStringHelper()
                .add("doubleStatistics", doubleStatistics);
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
        DoubleColumnStatistics that = (DoubleColumnStatistics) o;
        return equalsInternal(that) && Objects.equals(doubleStatistics, that.doubleStatistics);
    }

    public int hashCode()
    {
        return Objects.hash(super.hashCode(), doubleStatistics);
    }
}
