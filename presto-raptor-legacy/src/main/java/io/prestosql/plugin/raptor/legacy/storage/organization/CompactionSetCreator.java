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
package io.prestosql.plugin.raptor.legacy.storage.organization;

import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.prestosql.plugin.raptor.legacy.metadata.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.raptor.legacy.storage.organization.ShardOrganizerUtil.createOrganizationSet;
import static io.prestosql.plugin.raptor.legacy.storage.organization.ShardOrganizerUtil.getShardsByDaysBuckets;
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

        for (ShardIndexInfo shard : shards) {
            if (((consumedBytes + shard.getUncompressedSize()) > maxShardSize.toBytes()) ||
                    (consumedRows + shard.getRowCount() > maxShardRows)) {
                // Finalize this compaction set, and start a new one for the rest of the shards
                Set<ShardIndexInfo> shardsToCompact = builder.build();

                if (shardsToCompact.size() > 1) {
                    compactionSets.add(createOrganizationSet(tableId, shardsToCompact));
                }

                builder = ImmutableSet.builder();
                consumedBytes = 0;
                consumedRows = 0;
            }
            builder.add(shard);
            consumedBytes += shard.getUncompressedSize();
            consumedRows += shard.getRowCount();
        }

        // create compaction set for the remaining shards of this day
        Set<ShardIndexInfo> shardsToCompact = builder.build();
        if (shardsToCompact.size() > 1) {
            compactionSets.add(createOrganizationSet(tableId, shardsToCompact));
        }
        return compactionSets.build();
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
