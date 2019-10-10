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
import com.facebook.presto.spi.predicate.TupleDomain;
import javafx.util.Pair;
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
    void createTable(long tableId, boolean tableSupportsDeltaDelete, List<ColumnInfo> columns, boolean bucketed, OptionalLong temporalColumnId);

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
     * Used by rewrite delete and compaction.
     * With tableSupportsDeltaDelete: Delete oldShardsUuids with their delta shards
     * Add newShards
     */
    void replaceShardUuids(long transactionId, boolean tableSupportsDeltaDelete, long tableId, List<ColumnInfo> columns, Map<UUID, Optional<UUID>> oldShardUuids, Collection<ShardInfo> newShards, OptionalLong updateTime);

    /**
     * Replace oldDeltaDeleteShard with newDeltaDeleteShard.
     * Used by delta delete.
     * For shardMap:
     *      UUID is the target file.
     *      Optional<UUID> in the Pair is the oldDeltaDeleteShard for the target file.
     *      Optional<ShardInfo>> in the Pair is the newDeltaDeleteShard for the target file.
     * NOTE: Optional<ShardInfo>> being Optional.empty() means deleting the target file.
     */
    void replaceDeltaUuids(long transactionId, long tableId, List<ColumnInfo> columns, Map<UUID, Pair<Optional<UUID>, Optional<ShardInfo>>> shardMap, OptionalLong updateTime);

    /**
     * Get shard metadata for a shard.
     */
    ShardMetadata getShard(UUID shardUuid);

    /**
     * Get shard metadata for shards on a given node.
     */
    Set<ShardMetadata> getNodeShards(String nodeIdentifier);

    /**
     * Get shard metadata for shards on a given node.
     */
    Set<ShardMetadata> getNodeShards(String nodeIdentifier, long tableId);

    /**
     * Return the shard nodes for a non-bucketed table.
     */
    ResultIterator<BucketShards> getShardNodes(long tableId, boolean tableSupportsDeltaDelete, TupleDomain<RaptorColumnHandle> effectivePredicate);

    /**
     * Return the shard nodes for a bucketed table.
     */
    ResultIterator<BucketShards> getShardNodesBucketed(long tableId, boolean tableSupportsDeltaDelete, boolean merged, List<String> bucketToNode, TupleDomain<RaptorColumnHandle> effectivePredicate);

    /**
     * Remove all old shard assignments and assign a shard to a node
     */
    void replaceShardAssignment(long tableId, UUID shardUuid, String nodeIdentifier, boolean gracePeriod);

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
