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

import com.google.common.annotations.VisibleForTesting;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterArgumentFactory;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.util.UuidArguments.UuidArgumentFactory;

@RegisterArgumentFactory(UuidArgumentFactory.class)
public interface ShardManagerDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS nodes (\n" +
            "  node_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  node_identifier VARCHAR(255) NOT NULL,\n" +
            "  UNIQUE (node_identifier)\n" +
            ")")
    void createTableNodes();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS shards (\n" +
            "  shard_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  shard_uuid BINARY(16) NOT NULL,\n" +
            "  create_time DATETIME NOT NULL,\n" +
            "  UNIQUE (shard_uuid)\n" +
            ")")
    void createTableShards();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS shard_nodes (\n" +
            "  shard_id BIGINT NOT NULL,\n" +
            "  node_id BIGINT NOT NULL,\n" +
            "  PRIMARY KEY (shard_id, node_id),\n" +
            "  FOREIGN KEY (shard_id) REFERENCES shards (shard_id),\n" +
            "  FOREIGN KEY (node_id) REFERENCES nodes (node_id)\n" +
            ")")
    void createTableShardNodes();

    // TODO: FOREIGN KEY (table_id) REFERENCES tables (table_id)
    @SqlUpdate("CREATE TABLE IF NOT EXISTS table_shards (\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  shard_id BIGINT NOT NULL,\n" +
            "  PRIMARY KEY (table_id, shard_id),\n" +
            "  FOREIGN KEY (shard_id) REFERENCES shards (shard_id)\n" +
            ")")
    void createTableTableShards();

    @SqlUpdate("INSERT INTO nodes (node_identifier) VALUES (:nodeIdentifier)")
    void insertNode(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlUpdate("INSERT INTO shards (shard_uuid, create_time)\n" +
            "VALUES (:shardUuid, CURRENT_TIMESTAMP)")
    @GetGeneratedKeys
    long insertShard(@Bind("shardUuid") UUID shardUuid);

    @SqlUpdate("INSERT INTO shard_nodes (shard_id, node_id)\n" +
            "VALUES (:shardId, :nodeId)\n")
    void insertShardNode(@Bind("shardId") long shardId, @Bind("nodeId") long nodeId);

    @SqlUpdate("INSERT INTO table_shards (table_id, shard_id)\n" +
            "VALUES (:tableId, :shardId)\n")
    void insertTableShard(@Bind("tableId") long tableId, @Bind("shardId") long shardId);

    @SqlQuery("SELECT node_id FROM nodes WHERE node_identifier = :nodeIdentifier")
    Long getNodeId(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlQuery("SELECT s.shard_uuid, n.node_identifier\n" +
            "FROM table_shards ts\n" +
            "JOIN shard_nodes sn ON (ts.shard_id = sn.shard_id)\n" +
            "JOIN shards s ON (sn.shard_id = s.shard_id)\n" +
            "JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "WHERE ts.table_id = :tableId")
    @Mapper(ShardNode.Mapper.class)
    List<ShardNode> getShardNodes(@Bind("tableId") long tableId);

    @VisibleForTesting
    @SqlQuery("SELECT node_identifier FROM nodes")
    Set<String> getAllNodesInUse();

    @SqlUpdate("DELETE FROM table_shards WHERE table_id = :tableId")
    void dropTableShards(@Bind("tableId") long tableId);
}
