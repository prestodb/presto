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
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toCollection;

public final class CompactionSetCreator
{
    private final DataSize maxShardSize;

    public CompactionSetCreator(DataSize maxShardSize)
    {
        this.maxShardSize = checkNotNull(maxShardSize, "maxShardSize is null");
    }

    public Set<Set<ShardMetadata>> getCompactionSets(Set<ShardMetadata> shardMetadata)
    {
        // Filter out shards larger than allowed size and sort in descending order of data size
        List<ShardMetadata> shards = shardMetadata.stream()
                .filter(shard -> shard.getUncompressedSize() < maxShardSize.toBytes())
                .sorted(comparing(ShardMetadata::getUncompressedSize).reversed())
                .collect(toCollection(ArrayList::new));

        ImmutableSet.Builder<Set<ShardMetadata>> compactionSets = ImmutableSet.builder();
        while (!shards.isEmpty()) {
            Set<ShardMetadata> compactionSet = getCompactionSet(shards);
            verify(!compactionSet.isEmpty());
            compactionSets.add(compactionSet);
            shards.removeAll(compactionSet);
        }
        return compactionSets.build();
    }

    private Set<ShardMetadata> getCompactionSet(List<ShardMetadata> shardMetadata)
    {
        ImmutableSet.Builder<ShardMetadata> shards = ImmutableSet.builder();
        long maxShardSizeBytes = maxShardSize.toBytes();
        long consumedBytes = 0;
        for (ShardMetadata shard : shardMetadata) {
            long uncompressedSize = shard.getUncompressedSize();
            if (consumedBytes + uncompressedSize > maxShardSizeBytes) {
                break;
            }
            consumedBytes += uncompressedSize;
            shards.add(shard);
        }
        return shards.build();
    }
}
