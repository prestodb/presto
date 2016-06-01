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
import org.skife.jdbi.v2.ResultIterator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface ShardManager
{
    /**
     * Create a table.
     */
    void createTable(long tableId, List<ColumnInfo> columns, boolean bucketed);

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
    void commitShards(long transactionId, long tableId, List<ColumnInfo> columns, Collection<ShardInfo> shards, Optional<String> externalBatchId);

    /**
     * Replace oldShardsUuids with newShards.
     */
    void replaceShardUuids(long transactionId, long tableId, List<ColumnInfo> columns, Set<UUID> oldShardUuids, Collection<ShardInfo> newShards);

    /**
     * Get shard metadata for shards on a given node.
     */
    Set<ShardMetadata> getNodeShards(String nodeIdentifier);

    /**
     * Return the shard nodes for a non-bucketed table.
     */
    ResultIterator<BucketShards> getShardNodes(long tableId, TupleDomain<RaptorColumnHandle> effectivePredicate);

    /**
     * Return the shard nodes for a bucketed table.
     */
    ResultIterator<BucketShards> getShardNodesBucketed(long tableId, boolean merged, Map<Integer, String> bucketToNode, TupleDomain<RaptorColumnHandle> effectivePredicate);

    /**
     * Assign a shard to a node.
     */
    void assignShard(long tableId, UUID shardUuid, String nodeIdentifier, boolean gracePeriod);

    /**
     * Remove shard assignment from a node.
     */
    void unassignShard(long tableId, UUID shardUuid, String nodeIdentifier);

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
     * Get map of buckets to node identifiers for a table.
     */
    Map<Integer, String> getBucketAssignments(long distributionId);
}
