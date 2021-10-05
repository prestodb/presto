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

public class DateColumnStatistics
        extends ColumnStatistics
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DateColumnStatistics.class).instanceSize();

    private final DateStatistics dateStatistics;

    public DateColumnStatistics(
            Long numberOfValues,
            long minAverageValueSizeInBytes,
            HiveBloomFilter bloomFilter,
            DateStatistics dateStatistics)
    {
        super(numberOfValues, minAverageValueSizeInBytes, bloomFilter);
        this.dateStatistics = dateStatistics;
    }

    @Override
    public DateStatistics getDateStatistics()
    {
        return dateStatistics;
    }

    @Override
    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return new DateColumnStatistics(
                getNumberOfValues(),
                getMinAverageValueSizeInBytes(),
                bloomFilter,
                dateStatistics);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long sizeInBytes = INSTANCE_SIZE + getMembersSizeInBytes();
        if (dateStatistics != null) {
            return sizeInBytes + dateStatistics.getRetainedSizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        super.addHash(hasher);
        hasher.putOptionalHashable(dateStatistics);
    }

    @Override
    protected ToStringHelper getToStringHelper()
    {
        return super.getToStringHelper()
                .add("dateStatistics", dateStatistics);
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
        DateColumnStatistics that = (DateColumnStatistics) o;
        return equalsInternal(that) && Objects.equals(dateStatistics, that.dateStatistics);
    }

    public int hashCode()
    {
        return Objects.hash(super.hashCode(), dateStatistics);
    }
}
