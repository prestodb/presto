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
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;

public final class FileCompactionSetCreator
        implements CompactionSetCreator
{
    private final DataSize maxShardSize;
    private final long maxShardRows;

    public FileCompactionSetCreator(DataSize maxShardSize, long maxShardRows)
    {
        this.maxShardSize = requireNonNull(maxShardSize, "maxShardSize is null");
        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = maxShardRows;
    }

    @Override
    public Set<OrganizationSet> createCompactionSets(Table tableInfo, Collection<ShardIndexInfo> shardInfos)
    {
        ImmutableSet.Builder<OrganizationSet> compactionSets = ImmutableSet.builder();
        Multimap<OptionalInt, ShardIndexInfo> map = Multimaps.index(shardInfos, ShardIndexInfo::getBucketNumber);
        for (Collection<ShardIndexInfo> shards : map.asMap().values()) {
            compactionSets.addAll(buildCompactionSets(tableInfo.getTableId(), ImmutableSet.copyOf(shards)));
        }
        return compactionSets.build();
    }

    private Set<OrganizationSet> buildCompactionSets(long tableId, Set<ShardIndexInfo> shardInfos)
    {
        // Filter out shards larger than allowed size and sort in descending order of data size
        List<ShardIndexInfo> shards = shardInfos.stream()
                .filter(shard -> shard.getUncompressedSize() < maxShardSize.toBytes())
                .filter(shard -> shard.getRowCount() < maxShardRows)
                .sorted(comparing(ShardIndexInfo::getUncompressedSize).reversed())
                .collect(toCollection(ArrayList::new));

        ImmutableSet.Builder<OrganizationSet> compactionSets = ImmutableSet.builder();
        while (!shards.isEmpty()) {
            Set<ShardIndexInfo> compactionSet = getCompactionSet(shards);
            verify(!compactionSet.isEmpty());

            Set<OptionalInt> bucketNumber = compactionSet.stream()
                    .map(ShardIndexInfo::getBucketNumber)
                    .collect(toSet());
            verify(bucketNumber.size() == 1, "shards must belong to the same bucket");

            Set<UUID> shardUuids = compactionSet.stream()
                    .map(ShardIndexInfo::getShardUuid)
                    .collect(toSet());
            if (shardUuids.size() > 1) {
                compactionSets.add(new OrganizationSet(tableId, shardUuids, getOnlyElement(bucketNumber)));
            }

            shards.removeAll(compactionSet);
        }
        return compactionSets.build();
    }

    private Set<ShardIndexInfo> getCompactionSet(List<ShardIndexInfo> shardInfos)
    {
        ImmutableSet.Builder<ShardIndexInfo> shards = ImmutableSet.builder();
        long maxShardSizeBytes = maxShardSize.toBytes();
        long consumedBytes = 0;
        long consumedRows = 0;

        for (ShardIndexInfo shard : shardInfos) {
            if ((consumedBytes + shard.getUncompressedSize() > maxShardSizeBytes) ||
                    (consumedRows + shard.getRowCount() > maxShardRows)) {
                break;
            }
            consumedBytes += shard.getUncompressedSize();
            consumedRows += shard.getRowCount();
            shards.add(shard);
        }
        return shards.build();
    }
}
