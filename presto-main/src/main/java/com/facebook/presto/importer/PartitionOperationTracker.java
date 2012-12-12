package com.facebook.presto.importer;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Tracks which partitions still have outstanding tasks
 */
class PartitionOperationTracker
{
    private static final Logger log = Logger.get(PartitionOperationTracker.class);

    private final ConcurrentMap<PartitionMarker, AtomicInteger> importingPartitions = new ConcurrentHashMap<>();

    public Set<String> getOperatingPartitions(long tableId)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (PartitionMarker partitionMarker : importingPartitions.keySet()) {
            if (partitionMarker.getTableId() == tableId) {
                builder.add(partitionMarker.getPartitionName());
            }
        }
        return builder.build();
    }

    public boolean claimPartition(PartitionMarker partitionMarker, int totalTasks)
    {
        checkNotNull(partitionMarker, "partitionMarker is null");
        checkArgument(totalTasks > 0, "totalTasks must be greater than zero");
        return importingPartitions.putIfAbsent(partitionMarker, new AtomicInteger(totalTasks)) == null;
    }

    public void decrementActiveTasks(PartitionMarker partitionMarker)
    {
        checkNotNull(partitionMarker, "partitionMarker is null");
        AtomicInteger chunkCount = importingPartitions.get(partitionMarker);
        checkArgument(chunkCount != null, "unknown partitionMarker: %s", partitionMarker);
        assert chunkCount != null; // IDEA-60343
        checkState(chunkCount.get() > 0, "cannot decrement partitionMarker %s with count %s", partitionMarker, chunkCount.get());
        if (chunkCount.decrementAndGet() == 0) {
            checkState(importingPartitions.remove(partitionMarker, chunkCount));
            log.info("Operation finished for: " + partitionMarker);
        }
        checkState(chunkCount.get() >= 0);
    }
}
