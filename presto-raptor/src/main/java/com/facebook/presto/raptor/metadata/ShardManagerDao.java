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
import com.google.common.annotations.VisibleForTesting;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterArgumentFactory;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapperFactory;

import java.util.List;
import java.util.Set;
import java.util.UUID;

@RegisterArgumentFactory(UuidArgumentFactory.class)
@RegisterMapperFactory(UuidMapperFactory.class)
public interface ShardManagerDao
{
    @SqlUpdate("INSERT INTO nodes (node_identifier) VALUES (:nodeIdentifier)")
    @GetGeneratedKeys
    int insertNode(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlUpdate("INSERT INTO shards (shard_uuid, table_id, create_time, row_count, compressed_size, uncompressed_size)\n" +
            "VALUES (:shardUuid, :tableId, CURRENT_TIMESTAMP, :rowCount, :compressedSize, :uncompressedSize)")
    @GetGeneratedKeys
    long insertShard(
            @Bind("shardUuid") UUID shardUuid,
            @Bind("tableId") long tableId,
            @Bind("rowCount") long rowCount,
            @Bind("compressedSize") long compressedSize,
            @Bind("uncompressedSize") long uncompressedSize);

    @SqlUpdate("INSERT INTO shard_nodes (shard_id, node_id)\n" +
            "VALUES (:shardId, :nodeId)\n")
    void insertShardNode(@Bind("shardId") long shardId, @Bind("nodeId") int nodeId);

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
    @Mapper(Node.Mapper.class)
    List<Node> getNodes();

    @SqlQuery("SELECT shard_uuid FROM shards WHERE table_id = :tableId")
    List<UUID> getShards(@Bind("tableId") long tableId);

    @SqlQuery("SELECT s.table_id, s.shard_id, s.shard_uuid, s.row_count, s.compressed_size, s.uncompressed_size\n" +
            "FROM shards s\n" +
            "JOIN shard_nodes sn ON (s.shard_id = sn.shard_id)\n" +
            "JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "WHERE n.node_identifier = :nodeIdentifier")
    @Mapper(ShardMetadata.Mapper.class)
    Set<ShardMetadata> getNodeShards(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlQuery("SELECT s.shard_uuid, n.node_identifier\n" +
            "FROM shards s\n" +
            "JOIN shard_nodes sn ON (s.shard_id = sn.shard_id)\n" +
            "JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "WHERE s.table_id = :tableId")
    @Mapper(ShardNode.Mapper.class)
    List<ShardNode> getShardNodes(@Bind("tableId") long tableId);

    @VisibleForTesting
    @SqlQuery("SELECT node_identifier FROM nodes")
    Set<String> getAllNodesInUse();

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
}
