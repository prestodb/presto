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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.units.DataSize;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TemporalCompactionSetCreator
        implements CompactionSetCreator
{
    private final long maxShardSizeBytes;
    private final Type type;
    private final long maxShardRows;

    public TemporalCompactionSetCreator(DataSize maxShardSize, long maxShardRows, Type type)
    {
        requireNonNull(maxShardSize, "maxShardSize is null");
        checkArgument(type.equals(DATE) || type.equals(TIMESTAMP), "type must be timestamp or date");

        this.maxShardSizeBytes = maxShardSize.toBytes();

        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = maxShardRows;
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public Set<CompactionSet> createCompactionSets(long tableId, Set<ShardMetadata> shardMetadata)
    {
        if (shardMetadata.isEmpty()) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<CompactionSet> compactionSets = ImmutableSet.builder();
        // don't compact shards across days
        Multimap<Long, ShardMetadata> shardsByDays = getShardsByDays(shardMetadata, type);

        for (Collection<ShardMetadata> shardSet : shardsByDays.asMap().values()) {
            List<ShardMetadata> shards = shardSet.stream()
                    .filter(shard -> shard.getUncompressedSize() < maxShardSizeBytes)
                    .filter(shard -> shard.getRowCount() < maxShardRows)
                    .sorted(new ShardSorter())
                    .collect(toList());

            long consumedBytes = 0;
            long consumedRows = 0;
            ImmutableSet.Builder<ShardMetadata> shardsToCompact = ImmutableSet.builder();

            for (ShardMetadata shard : shards) {
                if (((consumedBytes + shard.getUncompressedSize()) > maxShardSizeBytes) ||
                        (consumedRows + shard.getRowCount() > maxShardRows)) {
                    // Finalize this compaction set, and start a new one for the rest of the shards
                    compactionSets.add(new CompactionSet(tableId, shardsToCompact.build()));
                    shardsToCompact = ImmutableSet.builder();
                    consumedBytes = 0;
                    consumedRows = 0;
                }
                shardsToCompact.add(shard);
                consumedBytes += shard.getUncompressedSize();
                consumedRows += shard.getRowCount();
            }
            if (!shardsToCompact.build().isEmpty()) {
                // create compaction set for the remaining shards of this day
                compactionSets.add(new CompactionSet(tableId, shardsToCompact.build()));
            }
        }
        return compactionSets.build();
    }

    private static Multimap<Long, ShardMetadata> getShardsByDays(Set<ShardMetadata> shardMetadata, Type type)
    {
        // bucket shards by the start day
        ImmutableMultimap.Builder<Long, ShardMetadata> shardsByDays = ImmutableMultimap.builder();

        // skip shards that do not have temporal information
        shardMetadata.stream()
                .filter(shard -> shard.getRangeStart().isPresent() && shard.getRangeEnd().isPresent())
                .forEach(shard -> {
                    long day = determineDay(shard.getRangeStart().getAsLong(), shard.getRangeEnd().getAsLong(), type);
                    shardsByDays.put(day, shard);
                });
        return shardsByDays.build();
    }

    private static long determineDay(long rangeStart, long rangeEnd, Type type)
    {
        if (type.equals(DATE)) {
            return rangeStart;
        }

        long startDay = Duration.ofMillis(rangeStart).toDays();
        long endDay = Duration.ofMillis(rangeEnd).toDays();
        if (startDay == endDay) {
            return startDay;
        }

        if ((endDay - startDay) > 1) {
            // range spans multiple days, return the first full day
            return startDay + 1;
        }

        // range spans two days, return the day that has the larger time range
        long millisInStartDay = Duration.ofDays(endDay).toMillis() - rangeStart;
        long millisInEndDay = rangeEnd - Duration.ofDays(endDay).toMillis();
        return (millisInStartDay >= millisInEndDay) ? startDay : endDay;
    }

    private static class ShardSorter
            implements Comparator<ShardMetadata>
    {
        @SuppressWarnings("SubtractionInCompareTo")
        @Override
        public int compare(ShardMetadata shard1, ShardMetadata shard2)
        {
            // sort shards first by the starting hour
            // for shards that start in the same hour, pick shards that have a shorter time range
            long shard1Hours = Duration.ofMillis(shard1.getRangeStart().getAsLong()).toHours();
            long shard2Hours = Duration.ofMillis(shard2.getRangeStart().getAsLong()).toHours();

            long shard1Range = shard1.getRangeEnd().getAsLong() - shard1.getRangeStart().getAsLong();
            long shard2Range = shard2.getRangeEnd().getAsLong() - shard2.getRangeStart().getAsLong();

            return ComparisonChain.start()
                    .compare(shard1Hours, shard2Hours)
                    .compare(shard1Range, shard2Range)
                    .result();
        }
    }
}
