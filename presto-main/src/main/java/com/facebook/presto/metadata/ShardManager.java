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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Optional;
import com.google.common.collect.Multimap;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public interface ShardManager
{
    /**
     * Allocate a new shard id for a table.
     */
    long allocateShard(TableHandle tableHandle);

    /**
     * Mark shard as complete with data residing on given node
     */
    void commitShard(long shardId, String nodeIdentifier);

    /**
     * Remove a shard from a node. When this method returns successfully, the shard will be no longer retrieved
     * from that node.
     */
    void disassociateShard(long shardId, @Nullable String nodeIdentifier);

    /**
     * Drop all information about a given shard.
     */
    void dropShard(long shardId);

    /**
     * Commit a partition for a table.
     */
    void commitPartition(TableHandle tableHandle, String partition, List<? extends PartitionKey> partitionKeys, Map<Long, String> shards);

    /**
     * Get the names of all partitions that have been successfully imported.
     *
     * @return list of partition names
     */
    Set<TablePartition> getPartitions(TableHandle tableHandle);

    /**
     * Get all partition keys by Partition for a given table handle.
     */
    Multimap<String, ? extends PartitionKey> getAllPartitionKeys(TableHandle tableHandle);

    /**
     * Return a map with all partition names to committed shard nodes for a given table.
     */
    Multimap<Long, Entry<Long, String>> getCommittedPartitionShardNodes(TableHandle tableHandle);

    /**
     * Get all complete shards in a table
     *
     * @return mapping of shard ID to node identifier
     */
    Multimap<Long, String> getCommittedShardNodesByTableId(TableHandle tableHandle);

    /**
     * Get all complete shards in table partition
     *
     * @return mapping of shard ID to node identifier
     */
    Multimap<Long, String> getShardNodes(long tableId, String partitionName);

    /**
     * Return a collection of all nodes that were used in this shard manager.
     */
    public Iterable<String> getAllNodesInUse();

    /**
     * Drop all record of the specified partition
     */
    void dropPartition(TableHandle tableHandle, String partitionName);

    /**
     * remove all partitions that are no longer referred from any shard.
     */
    void dropOrphanedPartitions();

    /**
     * Return a list of all shard ids for a given node that are no referenced by a table.
     */
    Iterable<Long> getOrphanedShardIds(Optional<String> nodeIdentifier);
}
