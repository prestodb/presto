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

import static java.util.Objects.requireNonNull;

public class DecimalColumnStatistics
        extends ColumnStatistics
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalColumnStatistics.class).instanceSize();

    private final DecimalStatistics decimalStatistics;

    public DecimalColumnStatistics(
            Long numberOfValues,
            HiveBloomFilter bloomFilter,
            DecimalStatistics decimalStatistics)
    {
        super(numberOfValues, bloomFilter);
        requireNonNull(decimalStatistics, "decimalStatistics is null");
        this.decimalStatistics = decimalStatistics;
    }

    @Override
    public DecimalStatistics getDecimalStatistics()
    {
        return decimalStatistics;
    }

    @Override
    public long getMinAverageValueSizeInBytes()
    {
        return decimalStatistics.getMinAverageValueSizeInBytes();
    }

    @Override
    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return new DecimalColumnStatistics(
                getNumberOfValues(),
                bloomFilter,
                decimalStatistics);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long sizeInBytes = INSTANCE_SIZE + getMembersSizeInBytes();
        return sizeInBytes + decimalStatistics.getRetainedSizeInBytes();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        super.addHash(hasher);
        hasher.putOptionalHashable(decimalStatistics);
    }

    @Override
    protected ToStringHelper getToStringHelper()
    {
        return super.getToStringHelper()
                .add("decimalStatistics", decimalStatistics);
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
        DecimalColumnStatistics that = (DecimalColumnStatistics) o;
        return equalsInternal(that) && Objects.equals(decimalStatistics, that.decimalStatistics);
    }

    public int hashCode()
    {
        return Objects.hash(super.hashCode(), decimalStatistics);
    }
}
