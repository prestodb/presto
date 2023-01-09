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

import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.orc.metadata.statistics.StatisticsHasher.Hashable;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MapStatistics
        implements Hashable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapStatistics.class).instanceSize();
    private final List<MapStatisticsEntry> entries;

    public MapStatistics(List<MapStatisticsEntry> entries)
    {
        this.entries = requireNonNull(entries, "entries is null");
    }

    public List<MapStatisticsEntry> getEntries()
    {
        return entries;
    }

    public long getRetainedSizeInBytes()
    {
        long entriesSize = 0;
        for (MapStatisticsEntry entry : entries) {
            entriesSize += entry.getRetainedSizeInBytes();
        }
        return INSTANCE_SIZE + entriesSize;
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
        MapStatistics that = (MapStatistics) o;
        return Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(entries);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("entries", entries)
                .toString();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        entries.forEach(hasher::putOptionalHashable);
    }
}
