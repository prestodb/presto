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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.util.CloseableIterator;
import com.facebook.presto.spi.TupleDomain;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface ShardManager
{
    /**
     * Create a table.
     */
    void createTable(long tableId, List<ColumnInfo> columns);

    /**
     * Commit data for a table.
     */
    void commitShards(long tableId, List<ColumnInfo> columns, Collection<ShardInfo> shards, Optional<String> externalBatchId);

    /**
     * Replace oldShardsIds with newShards.
     */
    void replaceShardIds(long tableId, List<ColumnInfo> columns, Set<Long> oldShardIds, Collection<ShardInfo> newShards);

    /**
     * Replace oldShardsUuids with newShards.
     */
    void replaceShardUuids(long tableId, List<ColumnInfo> columns, Set<UUID> oldShardUuids, Collection<ShardInfo> newShards);

    /**
     * Get shard metadata for table shards on a given node.
     */
    Set<ShardMetadata> getNodeTableShards(String nodeIdentifier, long tableId);

    /**
     * Return the shard nodes a given table.
     */
    CloseableIterator<ShardNodes> getShardNodes(long tableId, TupleDomain<RaptorColumnHandle> effectivePredicate);

    /**
     * Return the shards for a given node
     */
    Set<UUID> getNodeShards(String nodeIdentifier);

    /**
     * Drop all shards in a given table.
     */
    void dropTableShards(long tableId);

    /**
     * Assign a shard to a node.
     */
    void assignShard(long tableId, UUID shardUuid, String nodeIdentifier);
}
