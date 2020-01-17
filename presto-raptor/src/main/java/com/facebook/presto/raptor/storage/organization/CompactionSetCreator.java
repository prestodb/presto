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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.raptor.storage.organization.ShardOrganizerUtil.createOrganizationSet;
import static com.facebook.presto.raptor.storage.organization.ShardOrganizerUtil.getShardsByDaysBuckets;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class CompactionSetCreator
{
    private final DataSize maxShardSize;
    private final long maxShardRows;
    private final TemporalFunction temporalFunction;

    public CompactionSetCreator(TemporalFunction temporalFunction, DataSize maxShardSize, long maxShardRows)
    {
        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");

        this.temporalFunction = requireNonNull(temporalFunction, "temporalFunction is null");
        this.maxShardSize = requireNonNull(maxShardSize, "maxShardSize is null");
        this.maxShardRows = maxShardRows;
    }

    // Expects a pre-filtered collection of shards.
    // All shards provided to this method will be considered for creating a compaction set.
    public Set<OrganizationSet> createCompactionSets(Table tableInfo, Collection<ShardIndexInfo> shards)
    {
        Collection<Collection<ShardIndexInfo>> shardsByDaysBuckets = getShardsByDaysBuckets(tableInfo, shards, temporalFunction);

        ImmutableSet.Builder<OrganizationSet> compactionSets = ImmutableSet.builder();
        for (Collection<ShardIndexInfo> shardInfos : shardsByDaysBuckets) {
            compactionSets.addAll(buildCompactionSets(tableInfo, ImmutableSet.copyOf(shardInfos)));
        }
        return compactionSets.build();
    }

    private Set<OrganizationSet> buildCompactionSets(Table tableInfo, Set<ShardIndexInfo> shardIndexInfos)
    {
        long tableId = tableInfo.getTableId();
        List<ShardIndexInfo> shards = shardIndexInfos.stream()
                .sorted(getShardIndexInfoComparator(tableInfo))
                .collect(toCollection(ArrayList::new));

        long consumedBytes = 0;
        long consumedRows = 0;
        ImmutableSet.Builder<ShardIndexInfo> builder = ImmutableSet.builder();
        ImmutableSet.Builder<OrganizationSet> compactionSets = ImmutableSet.builder();

        int priority = 0;
        for (ShardIndexInfo shard : shards) {
            if (((consumedBytes + shard.getUncompressedSize()) > maxShardSize.toBytes()) ||
                    (consumedRows + shard.getRowCount() > maxShardRows)) {
                // Finalize this compaction set, and start a new one for the rest of the shards
                Set<ShardIndexInfo> shardsToCompact = builder.build();
                addToCompactionSets(compactionSets, shardsToCompact, tableId, tableInfo, priority);

                priority = 0;
                builder = ImmutableSet.builder();
                consumedBytes = 0;
                consumedRows = 0;
            }
            if (shard.getDeltaUuid().isPresent()) {
                priority += 1;
            }
            builder.add(shard);
            consumedBytes += shard.getUncompressedSize();
            consumedRows += shard.getRowCount();
        }

        // create compaction set for the remaining shards of this day
        Set<ShardIndexInfo> shardsToCompact = builder.build();
        addToCompactionSets(compactionSets, shardsToCompact, tableId, tableInfo, priority);
        return compactionSets.build();
    }

    private void addToCompactionSets(ImmutableSet.Builder<OrganizationSet> compactionSets, Set<ShardIndexInfo> shardsToCompact, long tableId, Table tableInfo, int priority)
    {
        // Add special rule for shard which is too big to compact with other shards but have delta to compact
        if (shardsToCompact.size() > 1 || shardsToCompact.stream().anyMatch(shard -> shard.getDeltaUuid().isPresent())) {
            compactionSets.add(createOrganizationSet(tableId, tableInfo.isTableSupportsDeltaDelete(), shardsToCompact, priority));
        }
    }

    private static Comparator<ShardIndexInfo> getShardIndexInfoComparator(Table tableInfo)
    {
        if (!tableInfo.getTemporalColumnId().isPresent()) {
            return comparing(ShardIndexInfo::getUncompressedSize);
        }

        return comparing(info -> info.getTemporalRange().get(),
                comparing(ShardRange::getMinTuple)
                        .thenComparing(ShardRange::getMaxTuple));
    }
}
