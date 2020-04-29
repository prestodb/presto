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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.raptor.RaptorColumnHandle;
import org.skife.jdbi.v2.ResultIterator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

public interface ShardManager
{
    /**
     * Create a table.
     */
    void createTable(long tableId, List<ColumnInfo> columns, boolean bucketed, OptionalLong temporalColumnId, boolean tableSupportsDeltaDelete);

    /**
     * Drop a table.
     */
    void dropTable(long tableId);

    /**
     * Add a column to the end of the table.
     */
    void addColumn(long tableId, ColumnInfo column);

    /**
     * Commit data for a table.
     */
    void commitShards(long transactionId, long tableId, List<ColumnInfo> columns, Collection<ShardInfo> shards, Optional<String> externalBatchId, long updateTime);

    /**
     * Replace oldShardsUuids with newShards.
     */
    void replaceShardUuids(long transactionId, long tableId, List<ColumnInfo> columns, Set<UUID> oldShardUuids, Collection<ShardInfo> newShards, OptionalLong updateTime);

    /**
     * Replace oldShardsUuids with newShards.
     * Used by compaction with tableSupportsDeltaDelete: Delete oldShardsUuids with their delta shards and add newShards formed by compaction
     * @param oldShardAndDeltaUuids oldShardsUuids with their delta shards
     * @param newShards newShards formed from compaction
     */
    void replaceShardUuids(long transactionId, long tableId, List<ColumnInfo> columns, Map<UUID, Optional<UUID>> oldShardAndDeltaUuids, Collection<ShardInfo> newShards, OptionalLong updateTime, boolean tableSupportsDeltaDelete);

    /**
     * Replace oldDeltaDeleteShard with newDeltaDeleteShard.
     * Used by delta delete.
     * @param shardMap UUID in the map is the target file. DeltaInfoPair in the map is the change of delta.
     */
    void replaceDeltaUuids(long transactionId, long tableId, List<ColumnInfo> columns, Map<UUID, DeltaInfoPair> shardMap, OptionalLong updateTime);

    /**
     * Get shard metadata for a shard.
     */
    ShardMetadata getShard(UUID shardUuid);

    /**
     * Get shard and delta metadata for shards on a given node.
     */
    Set<ShardMetadata> getNodeShardsAndDeltas(String nodeIdentifier);

    /**
     * Get only shard metadata for shards on a given node.
     * Note: shard metadata will contain its delta
     */
    Set<ShardMetadata> getNodeShards(String nodeIdentifier);

    /**
     * Get only shard metadata for shards on a given node.
     * Note: shard metadata will contain its delta
     */
    Set<ShardMetadata> getNodeShards(String nodeIdentifier, long tableId);

    /**
     * Return the shard nodes for a non-bucketed table.
     */
    ResultIterator<BucketShards> getShardNodes(long tableId, TupleDomain<RaptorColumnHandle> effectivePredicate, boolean tableSupportsDeltaDelete);

    /**
     * Return the shard nodes for a bucketed table.
     */
    ResultIterator<BucketShards> getShardNodesBucketed(long tableId, boolean merged, List<String> bucketToNode, TupleDomain<RaptorColumnHandle> effectivePredicate, boolean tableSupportsDeltaDelete);

    /**
     * Remove all old shard assignments and assign a shard to a node
     */
    void replaceShardAssignment(long tableId, UUID shardUuid, Optional<UUID> deltaUuid, String nodeIdentifier, boolean gracePeriod);

    /**
     * Get the number of bytes used by assigned shards per node.
     */
    Map<String, Long> getNodeBytes();

    /**
     * Begin a transaction for creating shards.
     *
     * @return transaction ID
     */
    long beginTransaction();

    /**
     * Rollback a transaction.
     */
    void rollbackTransaction(long transactionId);

    /**
     * Create initial bucket assignments for a distribution.
     */
    void createBuckets(long distributionId, int bucketCount);

    /**
     * Get map of buckets to node identifiers for a distribution.
     */
    List<String> getBucketAssignments(long distributionId);

    /**
     * Change the node a bucket is assigned to.
     */
    void updateBucketAssignment(long distributionId, int bucketNumber, String nodeId);

    /**
     * Get all active distributions.
     */
    List<Distribution> getDistributions();

    /**
     * Get total physical size of all tables in a distribution.
     */
    long getDistributionSizeInBytes(long distributionId);

    /**
     * Get list of bucket nodes for a distribution.
     */
    List<BucketNode> getBucketNodes(long distributionId);

    /**
     * Return the subset of shard uuids that exist
     */
    Set<UUID> getExistingShardUuids(long tableId, Set<UUID> shardUuids);
}
