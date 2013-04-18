package com.facebook.presto.metadata;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
            "  table_id BIGINT NOT NULL,\n" +
            "  committed BOOLEAN NOT NULL\n" +
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

    @SqlUpdate("CREATE TABLE IF NOT EXISTS table_partitions (\n" +
            "  partition_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  partition_name VARCHAR(255) NOT NULL,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  UNIQUE (table_id, partition_name)\n" +
            ")")
    void createTablePartitions();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS partition_shards (\n" +
            "  shard_id BIGINT NOT NULL,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  partition_id BIGINT NOT NULL,\n" +
            "  FOREIGN KEY (shard_id) REFERENCES shards (shard_id),\n" +
            "  FOREIGN KEY (partition_id) REFERENCES table_partitions (partition_id)\n" +
            ")")
    void createPartitionShards();

    @SqlUpdate("INSERT INTO nodes (node_identifier) VALUES (:nodeIdentifier)")
    void insertNode(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlUpdate("INSERT INTO shards (table_id, committed)\n" +
            "VALUES (:tableId, :committed)\n")
    @GetGeneratedKeys
    long insertShard(
            @Bind("tableId") long tableId,
            @Bind("committed") boolean committed);

    @SqlUpdate("INSERT INTO shard_nodes (shard_id, node_id)\n" +
            "VALUES (:shardId, :nodeId)\n")
    void insertShardNode(
            @Bind("shardId") long shardId,
            @Bind("nodeId") long nodeId);

    @SqlUpdate("INSERT INTO table_partitions (partition_name, table_id)\n" +
            "VALUES (:partitionName, :tableId)\n")
    @GetGeneratedKeys
    long insertPartition(
            @Bind("tableId") long tableId,
            @Bind("partitionName") String partitionName);

    @SqlUpdate("INSERT INTO partition_shards (shard_id, table_id, partition_id)\n" +
            "VALUES (:shardId, :tableId, :partitionId)\n")
    void insertPartitionShard(
            @Bind("shardId") long shardId,
            @Bind("tableId") long tableId,
            @Bind("partitionId") long partitionId);

    @SqlUpdate("UPDATE shards SET committed = true WHERE shard_id = :shardId")
    void commitShard(@Bind("shardId") long shardId);

    @SqlQuery("SELECT node_id FROM nodes WHERE node_identifier = :nodeIdentifier")
    Long getNodeId(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlQuery("SELECT partition_name\n" +
            " FROM table_partitions\n" +
            " WHERE table_id = :tableId\n")
    Set<String> getPartitions(@Bind("tableId") long tableId);

    @SqlQuery("SELECT s.shard_id, n.node_identifier\n" +
            "FROM shard_nodes sn\n" +
            "JOIN shards s ON (sn.shard_id = s.shard_id)\n" +
            "JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "WHERE s.committed IS TRUE\n" +
            "  AND s.shard_id IN (SELECT shard_id from partition_shards ps WHERE ps.table_id = s.table_id)\n" +
            "  AND s.table_id = :tableId\n")
    @Mapper(ShardNode.Mapper.class)
    List<ShardNode> getCommittedShardNodes(@Bind("tableId") long tableId);

    @SqlQuery("SELECT sn.shard_id, n.node_identifier\n" +
            "FROM table_partitions tp\n" +
            "JOIN partition_shards ps ON (tp.partition_id = ps.partition_id)\n" +
            "JOIN shard_nodes sn ON (ps.shard_id = sn.shard_id)\n" +
            "JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "WHERE tp.table_id = :tableId\n" +
            "  AND tp.partition_name = :partitionName")
    @Mapper(ShardNode.Mapper.class)
    List<ShardNode> getAllShardNodes(@Bind("tableId") long tableId, @Bind("partitionName") String partitionName);

    @SqlQuery("SELECT node_identifier from nodes")
    List<String> getAllNodesInUse();

    @SqlQuery("SELECT ps.shard_id\n" +
            "FROM table_partitions tp\n" +
            "JOIN partition_shards ps ON (tp.partition_id = ps.partition_id)\n" +
            "WHERE tp.table_id = :tableId\n" +
            "  AND tp.partition_name = :partitionName\n")
    List<Long> getAllShards(@Bind("tableId") long tableId, @Bind("partitionName") String partitionName);

    @SqlUpdate("DELETE FROM shard_nodes\n" +
            "WHERE shard_id = :shardId\n")
    void deleteShardFromShardNodes(@Bind("shardId") long shardId);

    @SqlUpdate("DELETE FROM partition_shards\n" +
            "WHERE shard_id = :shardId\n")
    void deleteShardFromPartitionShards(@Bind("shardId") long shardId);

    @SqlUpdate("DELETE FROM shards\n" +
            "  WHERE shard_id = :shardId\n")
    void deleteShard(@Bind("shardId") long shardId);

    @SqlUpdate("DELETE FROM shard_nodes\n" +
            "  WHERE shard_id = :shardId\n" +
            "  AND (:nodeIdentifier IS NULL OR node_id = (SELECT node_id FROM nodes WHERE node_identifier = :nodeIdentifier))")
    void dropShardNode(@Bind("shardId") long shardId, @Nullable @Bind("nodeIdentifier") String nodeIdentifier);

    @SqlUpdate("DELETE FROM table_partitions\n" +
            "WHERE table_id = :tableId\n" +
            "  AND partition_name = :partitionName\n")
    void dropPartition(@Bind("tableId") long tableId, @Bind("partitionName") String partitionName);

    @SqlQuery("SELECT s.shard_id FROM shards s\n" +
            " LEFT JOIN shard_nodes sn ON (s.shard_id = sn.shard_id)\n" +
            " LEFT JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "  WHERE s.shard_id NOT IN (SELECT shard_id FROM partition_shards ps WHERE ps.table_id = s.table_id)\n" +
            "  AND s.committed IS TRUE\n" +
            "  AND n.node_identifier = :nodeIdentifier")
    List<Long> getOrphanedShards(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlQuery("SELECT s.shard_id FROM shards s\n" +
            "  WHERE s.shard_id NOT IN (SELECT shard_id FROM partition_shards ps WHERE ps.table_id = s.table_id)\n" +
            "  AND s.shard_id NOT IN (SELECT DISTINCT shard_id from shard_nodes sn)\n" +
            "  AND s.committed IS TRUE\n")
    List<Long> getAllOrphanedShards();

    @SqlUpdate("DELETE FROM table_partitions\n" +
            "  WHERE table_id NOT IN (SELECT table_id FROM tables)\n" +
            "  AND partition_id NOT IN (SELECT partition_id FROM partition_shards)\n")
    void dropAllOrphanedPartitions();

    public static class Utils
    {
        public static final Logger log = Logger.get(ShardManagerDao.class);

        public static void createShardTablesWithRetry(ShardManagerDao dao)
                throws InterruptedException
        {
            Duration delay = new Duration(10, TimeUnit.SECONDS);
            while (true) {
                try {
                    createShardTables(dao);
                    return;
                }
                catch (UnableToObtainConnectionException e) {
                    log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", delay, e.getMessage());
                    Thread.sleep((long) delay.toMillis());
                }
            }
        }

        private static void createShardTables(ShardManagerDao dao)
        {
            dao.createTableNodes();
            dao.createTableShards();
            dao.createTableShardNodes();
            dao.createTablePartitions();
            dao.createPartitionShards();
        }
    }
}
