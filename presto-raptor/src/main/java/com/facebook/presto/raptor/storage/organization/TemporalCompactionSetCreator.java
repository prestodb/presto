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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.units.DataSize;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.storage.organization.ShardOrganizerUtil.getShardsByDaysBuckets;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

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
    public Set<OrganizationSet> createCompactionSets(long tableId, Set<ShardMetadata> shardMetadata)
    {
        if (shardMetadata.isEmpty()) {
            return ImmutableSet.of();
        }

        Set<OptionalInt> bucketNumbers = shardMetadata.stream()
                .map(ShardMetadata::getBucketNumber)
                .collect(toSet());
        checkArgument(bucketNumbers.size() == 1, "shards must belong to the same bucket");
        OptionalInt bucketNumber = Iterables.getOnlyElement(bucketNumbers);

        ImmutableSet.Builder<OrganizationSet> compactionSets = ImmutableSet.builder();
        // don't compact shards across days or buckets
        Collection<Collection<ShardMetadata>> shardSets = getShardsByDaysBuckets(shardMetadata, type);

        for (Collection<ShardMetadata> shardSet : shardSets) {
            List<ShardMetadata> shards = shardSet.stream()
                    .filter(shard -> shard.getUncompressedSize() < maxShardSizeBytes)
                    .filter(shard -> shard.getRowCount() < maxShardRows)
                    .sorted(new ShardSorter())
                    .collect(toList());

            long consumedBytes = 0;
            long consumedRows = 0;
            ImmutableSet.Builder<UUID> shardsToCompact = ImmutableSet.builder();

            for (ShardMetadata shard : shards) {
                if (((consumedBytes + shard.getUncompressedSize()) > maxShardSizeBytes) ||
                        (consumedRows + shard.getRowCount() > maxShardRows)) {
                    // Finalize this compaction set, and start a new one for the rest of the shards
                    compactionSets.add(new OrganizationSet(tableId, shardsToCompact.build(), bucketNumber));
                    shardsToCompact = ImmutableSet.builder();
                    consumedBytes = 0;
                    consumedRows = 0;
                }
                shardsToCompact.add(shard.getShardUuid());
                consumedBytes += shard.getUncompressedSize();
                consumedRows += shard.getRowCount();
            }
            if (!shardsToCompact.build().isEmpty()) {
                // create compaction set for the remaining shards of this day
                compactionSets.add(new OrganizationSet(tableId, shardsToCompact.build(), bucketNumber));
            }
        }
        return compactionSets.build();
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
