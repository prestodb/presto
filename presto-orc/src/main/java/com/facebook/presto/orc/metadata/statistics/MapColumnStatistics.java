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

public class MapColumnStatistics
        extends ColumnStatistics
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapColumnStatistics.class).instanceSize();
    private final MapStatistics mapStatistics;

    public MapColumnStatistics(Long numberOfValues, HiveBloomFilter bloomFilter, MapStatistics mapStatistics)
    {
        super(numberOfValues, bloomFilter);
        this.mapStatistics = requireNonNull(mapStatistics, "mapStatistics is null");
    }

    @Override
    public MapStatistics getMapStatistics()
    {
        return mapStatistics;
    }

    @Override
    public long getTotalValueSizeInBytes()
    {
        long size = 0;
        for (MapStatisticsEntry entry : mapStatistics.getEntries()) {
            size += entry.getColumnStatistics().getTotalValueSizeInBytes();
        }
        return size;
    }

    @Override
    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return new MapColumnStatistics(getNumberOfValues(), bloomFilter, mapStatistics);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getMembersSizeInBytes() + mapStatistics.getRetainedSizeInBytes();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        super.addHash(hasher);
        hasher.putOptionalHashable(mapStatistics);
    }

    @Override
    protected ToStringHelper getToStringHelper()
    {
        return super.getToStringHelper()
                .add("mapStatistics", mapStatistics);
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
        MapColumnStatistics that = (MapColumnStatistics) o;
        return equalsInternal(that) && Objects.equals(mapStatistics, that.mapStatistics);
    }

    public int hashCode()
    {
        return Objects.hash(super.hashCode(), mapStatistics);
    }
}
