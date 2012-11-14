package com.facebook.presto.metadata;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.google.common.collect.Multimap;

import java.util.List;

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
    List<Long> createImportPartition(long tableId, String partitionName, List<SerializedPartitionChunk> partitionChunks);

    /**
     * Mark shard as complete with data residing on given node
     */
    void commitShard(long shardId, String nodeIdentifier);

    /**
     * Get all complete shards in table
     *
     * @return mapping of shard ID to node identifier
     */
    Multimap<Long, String> getShardNodes(long tableId);
}
