package com.facebook.presto.metadata;

import com.google.common.base.Optional;
import com.google.common.collect.Multimap;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Set;

public interface ShardManager
{
    /**
     * Allocate a new shard id for a table.
     */
    long allocateShard(TableHandle tableHandle);

    /**
     * Mark shard as complete with data residing on given node
     */
    void commitShard(long shardId, String nodeIdentifier);

    /**
     * Remove a shard from a node. When this method returns successfully, the shard will be no longer retrieved
     * from that node.
     */
    void disassociateShard(long shardId, @Nullable String nodeIdentifier);

    /**
     * Drop all information about a given shard.
     */
    void dropShard(long shardId);

    /**
     * Commit a partition for a table.
     */
    void commitPartition(TableHandle tableHandle, String partition, Map<Long, String> shards);

    /**
     * Get the names of all partitions that have been successfully imported.
     *
     * @return list of partition names
     */
    Set<String> getPartitions(TableHandle tableHandle);

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
     * Return a collection of all nodes that were used in this shard manager.
     */
    public Iterable<String> getAllNodesInUse();

    /**
     * Drop all record of the specified partition
     */
    void dropPartition(long tableId, String partitionName);

    /**
     * Return a list of all shard ids for a given node that are no referenced by a table.
     */
    Iterable<Long> getOrphanedShardIds(Optional<String> nodeIdentifier);
}
