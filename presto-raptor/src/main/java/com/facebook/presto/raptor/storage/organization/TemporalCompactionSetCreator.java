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

import com.facebook.presto.raptor.metadata.Table;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.storage.organization.ShardOrganizerUtil.getShardsByDaysBuckets;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class TemporalCompactionSetCreator
        implements CompactionSetCreator
{
    private final long maxShardSizeBytes;
    private final long maxShardRows;

    public TemporalCompactionSetCreator(DataSize maxShardSize, long maxShardRows)
    {
        requireNonNull(maxShardSize, "maxShardSize is null");
        this.maxShardSizeBytes = maxShardSize.toBytes();

        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = maxShardRows;
    }

    @Override
    public Set<OrganizationSet> createCompactionSets(Table tableInfo, Collection<ShardIndexInfo> shardMetadata)
    {
        if (shardMetadata.isEmpty()) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<OrganizationSet> compactionSets = ImmutableSet.builder();
        // don't compact shards across days or buckets
        Collection<Collection<ShardIndexInfo>> shardSets = getShardsByDaysBuckets(tableInfo, shardMetadata);

        for (Collection<ShardIndexInfo> shardSet : shardSets) {
            Set<OptionalInt> bucketNumbers = shardSet.stream()
                    .map(ShardIndexInfo::getBucketNumber)
                    .collect(toSet());
            verify(bucketNumbers.size() == 1, "shards must belong to the same bucket");
            OptionalInt bucketNumber = getOnlyElement(bucketNumbers);

            List<ShardIndexInfo> shards = shardSet.stream()
                    .filter(shard -> shard.getUncompressedSize() < maxShardSizeBytes)
                    .filter(shard -> shard.getRowCount() < maxShardRows)
                    .sorted(new ShardSorter())
                    .collect(toList());

            long consumedBytes = 0;
            long consumedRows = 0;
            ImmutableSet.Builder<UUID> shardsToCompact = ImmutableSet.builder();

            for (ShardIndexInfo shard : shards) {
                if (((consumedBytes + shard.getUncompressedSize()) > maxShardSizeBytes) ||
                        (consumedRows + shard.getRowCount() > maxShardRows)) {
                    // Finalize this compaction set, and start a new one for the rest of the shards
                    Set<UUID> shardUuids = shardsToCompact.build();
                    compactionSets.add(new OrganizationSet(tableInfo.getTableId(), shardUuids, bucketNumber));
                    shardsToCompact = ImmutableSet.builder();
                    consumedBytes = 0;
                    consumedRows = 0;
                }
                shardsToCompact.add(shard.getShardUuid());
                consumedBytes += shard.getUncompressedSize();
                consumedRows += shard.getRowCount();
            }
            Set<UUID> shardUuids = shardsToCompact.build();
            if (shardUuids.size() > 1) {
                // create compaction set for the remaining shards of this day
                compactionSets.add(new OrganizationSet(tableInfo.getTableId(), shardUuids, bucketNumber));
            }
        }
        return compactionSets.build();
    }

    private static class ShardSorter
            implements Comparator<ShardIndexInfo>
    {
        @SuppressWarnings("SubtractionInCompareTo")
        @Override
        public int compare(ShardIndexInfo shard1, ShardIndexInfo shard2)
        {
            checkArgument(shard1.getTemporalRange().isPresent() && shard2.getTemporalRange().isPresent());
            ShardRange range1 = shard1.getTemporalRange().get();
            ShardRange range2 = shard2.getTemporalRange().get();

            if (range1.getMinTuple().compareTo(range2.getMinTuple()) == 0) {
                return range1.getMaxTuple().compareTo(range2.getMaxTuple());
            }
            return range1.getMinTuple().compareTo(range2.getMinTuple());
        }
    }
}
