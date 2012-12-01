package com.facebook.presto.metadata;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;
import java.util.Set;

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
            "  FOREIGN KEY (shard_id) REFERENCES shards,\n" +
            "  FOREIGN KEY (node_id) REFERENCES nodes\n" +
            ")")
    void createTableShardNodes();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS import_tables (\n" +
            "  table_id BIGINT PRIMARY KEY,\n" +
            "  source_name VARCHAR(255) NOT NULL,\n" +
            "  database_name VARCHAR(255) NOT NULL,\n" +
            "  table_name VARCHAR(255) NOT NULL\n" +
            ")")
    void createTableImportTables();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS import_partitions (\n" +
            "  import_partition_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  partition_name VARCHAR(255) NOT NULL,\n" +
            "  UNIQUE (table_id, partition_name),\n" +
            "  FOREIGN KEY (table_id) REFERENCES import_tables\n" +
            ")")
    void createTableImportPartitions();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS import_partition_shards (\n" +
            "  import_partition_id BIGINT,\n" +
            "  shard_id BIGINT NOT NULL,\n" +
            "  partition_chunk VARBINARY(65535) NOT NULL,\n" +
            "  PRIMARY KEY (import_partition_id, shard_id),\n" +
            "  FOREIGN KEY (import_partition_id) REFERENCES import_partitions,\n" +
            "  FOREIGN KEY (shard_id) REFERENCES shards\n" +
            ")")
    void createTableImportPartitionShards();

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

    @SqlUpdate("INSERT INTO import_tables\n" +
            "(table_id, source_name, database_name, table_name)\n" +
            "VALUES (:tableId, :sourceName, :databaseName, :tableName)")
    void insertImportTable(
            @Bind("tableId") long tableId,
            @Bind("sourceName") String sourceName,
            @Bind("databaseName") String databaseName,
            @Bind("tableName") String tableName);

    @SqlUpdate("INSERT INTO import_partitions\n" +
            "(table_id, partition_name)\n" +
            "VALUES (:tableId, :partitionName)")
    @GetGeneratedKeys
    long insertImportPartition(
            @Bind("tableId") long tableId,
            @Bind("partitionName") String partitionName);

    @SqlUpdate("INSERT INTO import_partition_shards\n" +
            "(import_partition_id, shard_id, partition_chunk)\n" +
            "VALUES (:importPartitionId, :shardId, :partitionChunk)")
    void insertImportPartitionShard(
            @Bind("importPartitionId") long importPartitionId,
            @Bind("shardId") long shardId,
            @Bind("partitionChunk") byte[] partitionChunk);

    @SqlUpdate("UPDATE shards SET committed = true WHERE shard_id = :shardId")
    void commitShard(@Bind("shardId") long shardId);

    @SqlQuery("SELECT node_id FROM nodes WHERE node_identifier = :nodeIdentifier")
    Long getNodeId(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlQuery("SELECT COUNT(*) > 0 FROM import_tables WHERE table_id = :tableId")
    boolean importTableExists(@Bind("tableId") long tableId);

    @SqlQuery("SELECT partition_name\n" +
            "FROM import_partitions\n" +
            "WHERE table_id = :tableId\n")
    Set<String> getImportedPartitions(@Bind("tableId") long tableId);

    @SqlQuery("SELECT s.shard_id, n.node_identifier\n" +
            "FROM shard_nodes sn\n" +
            "JOIN shards s ON (sn.shard_id = s.shard_id)\n" +
            "JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "WHERE s.committed = true\n" +
            "  AND s.table_id = :tableId\n")
    @Mapper(ShardNode.Mapper.class)
    List<ShardNode> getShardNodes(@Bind("tableId") long tableId);
}
