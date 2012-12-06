package com.facebook.presto.metadata;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Set;

public interface ShardManager
{
    /**
     * Register table to be imported
     */
    void createImportTable(long tableId, String sourceName, String databaseName, String tableName);

    /**
     * Register partition (and all of it's chunks) to be imported
     *
     * @return list of shard IDs corresponding to partition chunks
     */
    List<Long> createImportPartition(long tableId, String partitionName, Iterable<SerializedPartitionChunk> partitionChunks);

    /**
     * Mark shard as complete with data residing on given node
     */
    void commitShard(long shardId, String nodeIdentifier);

    /**
     * Get the names of all current partitions that have started importing for table (and possibly finished or errored out).
     *
     * @return list of partition names
     */
    Set<String> getAllPartitions(long tableId);

    /**
     * Get all complete partitions in table
     *
     * @return list of partition names
     */
    Set<String> getCommittedPartitions(long tableId);

    /**
     * Get all complete shards in table
     *
     * @return mapping of shard ID to node identifier
     */
    Multimap<Long, String> getCommittedShardNodes(long tableId);

    /**
     * Get all complete shards in table partition
     *
     * @return mapping of shard ID to node identifier
     */
    Multimap<Long, String> getShardNodes(long tableId, String partitionName);

    /**
     * Drop all record of the specified partition
     */
    void dropPartition(long tableId, String partitionName);
}
