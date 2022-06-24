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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.proto.DwrfProto;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static java.util.Objects.requireNonNull;

public class MapColumnStatisticsBuilder
        implements StatisticsBuilder
{
    private long nonNullValueCount;
    private boolean hasEntries;
    private final ImmutableList.Builder<MapStatisticsEntry> entries = new ImmutableList.Builder<>();

    @Override
    public void addBlock(Type type, Block block)
    {
        throw new UnsupportedOperationException();
    }

    // Note: MapColumnStatisticsBuilder doesn't check the uniqueness of the keys
    public void addMapStatistics(DwrfProto.KeyInfo key, ColumnStatistics columnStatistics)
    {
        requireNonNull(key, "key is null");
        requireNonNull(columnStatistics, "columnStatistics is null");
        nonNullValueCount += columnStatistics.getNumberOfValues();
        hasEntries = true;
        entries.add(new MapStatisticsEntry(key, columnStatistics));
    }

    private Optional<MapStatistics> buildMapStatistics()
    {
        if (hasEntries) {
            MapStatistics mapStatistics = new MapStatistics(entries.build());
            return Optional.of(mapStatistics);
        }
        return Optional.empty();
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        if (hasEntries) {
            MapStatistics mapStatistics = new MapStatistics(entries.build());
            return new MapColumnStatistics(nonNullValueCount, null, mapStatistics);
        }
        return new ColumnStatistics(nonNullValueCount, null);
    }

    public static Optional<MapStatistics> mergeMapStatistics(List<ColumnStatistics> stats)
    {
        Map<DwrfProto.KeyInfo, List<ColumnStatistics>> columnStatisticsByKey = new LinkedHashMap<>();

        for (ColumnStatistics columnStatistics : stats) {
            if (columnStatistics.getNumberOfValues() > 0) {
                MapStatistics partialStatistics = columnStatistics.getMapStatistics();
                if (partialStatistics == null) {
                    // there are non-null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }

                // collect column stats for each key for merging later
                for (MapStatisticsEntry entry : partialStatistics.getEntries()) {
                    List<ColumnStatistics> allKeyStats = columnStatisticsByKey.computeIfAbsent(entry.getKey(), (k) -> new ArrayList<>());
                    allKeyStats.add(entry.getColumnStatistics());
                }
            }
        }

        // merge all column stats for each key
        MapColumnStatisticsBuilder mapStatisticsBuilder = new MapColumnStatisticsBuilder();
        for (Map.Entry<DwrfProto.KeyInfo, List<ColumnStatistics>> entry : columnStatisticsByKey.entrySet()) {
            ColumnStatistics mergedColumnStatistics = mergeColumnStatistics(entry.getValue());
            DwrfProto.KeyInfo key = entry.getKey();
            mapStatisticsBuilder.addMapStatistics(key, mergedColumnStatistics);
        }

        return mapStatisticsBuilder.buildMapStatistics();
    }
}
