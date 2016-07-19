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

import com.facebook.presto.raptor.util.UuidUtil.UuidArgumentFactory;
import com.facebook.presto.raptor.util.UuidUtil.UuidMapperFactory;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterArgumentFactory;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapperFactory;

import java.sql.Timestamp;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@RegisterArgumentFactory(UuidArgumentFactory.class)
@RegisterMapperFactory(UuidMapperFactory.class)
public interface ShardDao
{
    int CLEANABLE_SHARDS_BATCH_SIZE = 1000;

    @SqlUpdate("INSERT INTO nodes (node_identifier) VALUES (:nodeIdentifier)")
    @GetGeneratedKeys
    int insertNode(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlUpdate("INSERT INTO shard_nodes (shard_id, node_id)\n" +
            "VALUES ((SELECT shard_id FROM shards WHERE shard_uuid = :shardUuid), :nodeId)")
    void insertShardNode(@Bind("shardUuid") UUID shardUuid, @Bind("nodeId") int nodeId);

    @SqlUpdate("DELETE FROM shard_nodes\n" +
            "WHERE shard_id = (SELECT shard_id FROM shards WHERE shard_uuid = :shardUuid)\n" +
            "  AND node_id = :nodeId")
    void deleteShardNode(@Bind("shardUuid") UUID shardUuid, @Bind("nodeId") int nodeId);

    @SqlQuery("SELECT node_id FROM nodes WHERE node_identifier = :nodeIdentifier")
    Integer getNodeId(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlQuery("SELECT node_identifier FROM nodes WHERE node_id = :nodeId")
    String getNodeIdentifier(@Bind("nodeId") int nodeId);

    @SqlQuery("SELECT node_id, node_identifier FROM nodes")
    @Mapper(RaptorNode.Mapper.class)
    List<RaptorNode> getNodes();

    @SqlQuery("SELECT table_id, shard_id, shard_uuid, bucket_number, row_count, compressed_size, uncompressed_size\n" +
            "FROM (\n" +
            "    SELECT s.*\n" +
            "    FROM shards s\n" +
            "    JOIN shard_nodes sn ON (s.shard_id = sn.shard_id)\n" +
            "    JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "    WHERE n.node_identifier = :nodeIdentifier\n" +
            "      AND s.bucket_number IS NULL\n" +
            "      AND (s.table_id = :tableId OR :tableId IS NULL)\n" +
            "  UNION ALL\n" +
            "    SELECT s.*\n" +
            "    FROM shards s\n" +
            "    JOIN tables t ON (s.table_id = t.table_id)\n" +
            "    JOIN distributions d ON (t.distribution_id = d.distribution_id)\n" +
            "    JOIN buckets b ON (\n" +
            "      d.distribution_id = b.distribution_id AND\n" +
            "      s.bucket_number = b.bucket_number)\n" +
            "    JOIN nodes n ON (b.node_id = n.node_id)\n" +
            "    WHERE n.node_identifier = :nodeIdentifier\n" +
            "      AND (s.table_id = :tableId OR :tableId IS NULL)\n" +
            ") x")
    @Mapper(ShardMetadata.Mapper.class)
    Set<ShardMetadata> getNodeShards(@Bind("nodeIdentifier") String nodeIdentifier, @Bind("tableId") Long tableId);

    @SqlQuery("SELECT n.node_identifier, x.bytes\n" +
            "FROM (\n" +
            "  SELECT node_id, sum(compressed_size) bytes\n" +
            "  FROM (\n" +
            "      SELECT sn.node_id, s.compressed_size\n" +
            "      FROM shards s\n" +
            "      JOIN shard_nodes sn ON (s.shard_id = sn.shard_id)\n" +
            "      WHERE s.bucket_number IS NULL\n" +
            "    UNION ALL\n" +
            "      SELECT b.node_id, s.compressed_size\n" +
            "      FROM shards s\n" +
            "      JOIN tables t ON (s.table_id = t.table_id)\n" +
            "      JOIN distributions d ON (t.distribution_id = d.distribution_id)\n" +
            "      JOIN buckets b ON (\n" +
            "        d.distribution_id = b.distribution_id AND\n" +
            "        s.bucket_number = b.bucket_number)\n" +
            "  ) x\n" +
            "  GROUP BY node_id\n" +
            ") x\n" +
            "JOIN nodes n ON (x.node_id = n.node_id)")
    @Mapper(NodeSize.Mapper.class)
    Set<NodeSize> getNodeSizes();

    @SqlUpdate("DELETE FROM shard_nodes WHERE shard_id IN (\n" +
            "  SELECT shard_id\n" +
            "  FROM shards\n" +
            "  WHERE table_id = :tableId)")
    void dropShardNodes(@Bind("tableId") long tableId);

    @SqlUpdate("DELETE FROM shards WHERE table_id = :tableId")
    void dropShards(@Bind("tableId") long tableId);

    @SqlUpdate("INSERT INTO external_batches (external_batch_id, successful)\n" +
            "VALUES (:externalBatchId, TRUE)")
    void insertExternalBatch(@Bind("externalBatchId") String externalBatchId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM external_batches\n" +
            "WHERE external_batch_id = :externalBatchId")
    boolean externalBatchExists(@Bind("externalBatchId") String externalBatchId);

    @SqlUpdate("INSERT INTO transactions (start_time) VALUES (CURRENT_TIMESTAMP)")
    @GetGeneratedKeys
    long insertTransaction();

    @SqlUpdate("UPDATE transactions SET\n" +
            "  successful = :successful\n" +
            ", end_time = CURRENT_TIMESTAMP\n" +
            "WHERE transaction_id = :transactionId\n" +
            "  AND successful IS NULL")
    int finalizeTransaction(
            @Bind("transactionId") long transactionId,
            @Bind("successful") boolean successful);

    @SqlQuery("SELECT successful FROM transactions WHERE transaction_id = :transactionId")
    Boolean transactionSuccessful(@Bind("transactionId") long transactionId);

    @SqlUpdate("UPDATE transactions SET\n" +
            "  successful = FALSE\n" +
            ", end_time = CURRENT_TIMESTAMP\n" +
            "WHERE successful IS NULL\n" +
            "  AND start_time < :maxStartTime")
    void abortOldTransactions(@Bind("maxStartTime") Timestamp maxStartTime);

    @SqlUpdate("INSERT INTO created_shards (shard_uuid, transaction_id)\n" +
            "VALUES (:shardUuid, :transactionId)")
    void insertCreatedShard(
            @Bind("shardUuid") UUID shardUuid,
            @Bind("transactionId") long transactionId);

    @SqlUpdate("DELETE FROM created_shards WHERE transaction_id = :transactionId")
    void deleteCreatedShards(@Bind("transactionId") long transactionId);

    @SqlBatch("DELETE FROM created_shards WHERE shard_uuid = :shardUuid")
    void deleteCreatedShards(@Bind("shardUuid") Iterable<UUID> shardUuids);

    void insertDeletedShards(Iterable<UUID> shardUuids);

    @SqlUpdate("INSERT INTO deleted_shards (shard_uuid, delete_time)\n" +
            "SELECT shard_uuid, CURRENT_TIMESTAMP\n" +
            "FROM shards\n" +
            "WHERE table_id = :tableId")
    void insertDeletedShards(@Bind("tableId") long tableId);

    @SqlQuery("SELECT s.shard_uuid\n" +
            "FROM created_shards s\n" +
            "JOIN transactions t ON (s.transaction_id = t.transaction_id)\n" +
            "WHERE NOT t.successful\n" +
            "LIMIT 10000")
    List<UUID> getOldCreatedShardsBatch();

    @SqlQuery("SELECT shard_uuid\n" +
            "FROM deleted_shards\n" +
            "WHERE delete_time < :maxDeleteTime\n" +
            "LIMIT " + CLEANABLE_SHARDS_BATCH_SIZE)
    Set<UUID> getCleanableShardsBatch(@Bind("maxDeleteTime") Timestamp maxDeleteTime);

    @SqlBatch("DELETE FROM deleted_shards WHERE shard_uuid = :shardUuid")
    void deleteCleanedShards(@Bind("shardUuid") Iterable<UUID> shardUuids);

    @SqlBatch("INSERT INTO buckets (distribution_id, bucket_number, node_id)\n" +
            "VALUES (:distributionId, :bucketNumber, :nodeId)\n")
    void insertBuckets(
            @Bind("distributionId") long distributionId,
            @Bind("bucketNumber") List<Integer> bucketNumbers,
            @Bind("nodeId") List<Integer> nodeIds);

    @SqlQuery("SELECT b.bucket_number, n.node_identifier\n" +
            "FROM buckets b\n" +
            "JOIN nodes n ON (b.node_id = n.node_id)\n" +
            "WHERE b.distribution_id = :distributionId\n" +
            "ORDER BY b.bucket_number")
    @Mapper(BucketNode.Mapper.class)
    List<BucketNode> getBucketNodes(@Bind("distributionId") long distributionId);

    @SqlUpdate("UPDATE buckets SET node_id = :nodeId\n" +
            "WHERE distribution_id = :distributionId\n" +
            "  AND bucket_number = :bucketNumber")
    void updateBucketNode(
            @Bind("distributionId") long distributionId,
            @Bind("bucketNumber") int bucketNumber,
            @Bind("nodeId") int nodeId);
}
