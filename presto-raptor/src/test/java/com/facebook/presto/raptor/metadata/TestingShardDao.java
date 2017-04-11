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
interface TestingShardDao
        extends H2ShardDao
{
    @SqlQuery("SELECT shard_uuid FROM shards WHERE table_id = :tableId")
    List<UUID> getShards(@Bind("tableId") long tableId);

    @SqlQuery("SELECT s.shard_uuid, n.node_identifier\n" +
            "FROM shards s\n" +
            "JOIN shard_nodes sn ON (s.shard_id = sn.shard_id)\n" +
            "JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "WHERE s.table_id = :tableId")
    @Mapper(ShardNode.Mapper.class)
    List<ShardNode> getShardNodes(@Bind("tableId") long tableId);

    @SqlQuery("SELECT node_identifier FROM nodes")
    Set<String> getAllNodesInUse();

    @SqlUpdate("INSERT INTO shards (shard_uuid, table_id, bucket_number, create_time, row_count, compressed_size, uncompressed_size)\n" +
            "VALUES (:shardUuid, :tableId, :bucketNumber, CURRENT_TIMESTAMP, :rowCount, :compressedSize, :uncompressedSize)")
    @GetGeneratedKeys
    long insertShard(
            @Bind("shardUuid") UUID shardUuid,
            @Bind("tableId") long tableId,
            @Bind("bucketNumber") Integer bucketNumber,
            @Bind("rowCount") long rowCount,
            @Bind("compressedSize") long compressedSize,
            @Bind("uncompressedSize") long uncompressedSize);

    @SqlUpdate("INSERT INTO shard_nodes (shard_id, node_id)\n" +
            "VALUES (:shardId, :nodeId)\n")
    void insertShardNode(@Bind("shardId") long shardId, @Bind("nodeId") int nodeId);
}
