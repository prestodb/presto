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

import static com.facebook.presto.orc.metadata.statistics.BooleanStatistics.BOOLEAN_VALUE_BYTES;
import static java.util.Objects.requireNonNull;

public class BooleanColumnStatistics
        extends ColumnStatistics
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BooleanColumnStatistics.class).instanceSize();

    private final BooleanStatistics booleanStatistics;

    public BooleanColumnStatistics(
            Long numberOfValues,
            HiveBloomFilter bloomFilter,
            BooleanStatistics booleanStatistics)
    {
        super(numberOfValues, bloomFilter);
        requireNonNull(booleanStatistics, "booleanStatistics is null");
        this.booleanStatistics = booleanStatistics;
    }

    @Override
    public BooleanStatistics getBooleanStatistics()
    {
        return booleanStatistics;
    }

    @Override
    public long getMinAverageValueSizeInBytes()
    {
        return BOOLEAN_VALUE_BYTES;
    }

    @Override
    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return new BooleanColumnStatistics(
                getNumberOfValues(),
                bloomFilter,
                booleanStatistics);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long sizeInBytes = INSTANCE_SIZE + super.getMembersSizeInBytes();
        return sizeInBytes + booleanStatistics.getRetainedSizeInBytes();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        super.addHash(hasher);
        hasher.putOptionalHashable(booleanStatistics);
    }

    @Override
    protected ToStringHelper getToStringHelper()
    {
        return super.getToStringHelper()
                .add("booleanStatistics", booleanStatistics);
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
        BooleanColumnStatistics that = (BooleanColumnStatistics) o;
        return equalsInternal(that) && Objects.equals(booleanStatistics, that.booleanStatistics);
    }

    public int hashCode()
    {
        return Objects.hash(super.hashCode(), booleanStatistics);
    }
}
