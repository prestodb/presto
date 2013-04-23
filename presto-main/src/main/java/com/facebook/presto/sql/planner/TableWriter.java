package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.operator.TableWriterResult;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.split.NativeSplit;
import com.facebook.presto.split.PartitionedSplit;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableWriter
{
    private final TableWriterNode tableWriterNode;
    private final ShardManager shardManager;

    // Which shards are part of which partition
    private final Map<String, Set<Long>> openPartitions = new ConcurrentHashMap<>();
    private final Map<String, Set<Long>> finishedPartitions = new ConcurrentHashMap<>();

    // Which shards have already been written to disk and where.
    private final Map<Long, String> shardsDone = new ConcurrentHashMap<Long, String>();

    // Which partitions have already been committed.
    private final Set<String> partitionsDone = Sets.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    private final AtomicInteger shardsInFlight = new AtomicInteger();

    private AtomicBoolean predicateHandedOut = new AtomicBoolean();

    // After finishing iteration over all source partitions, this set contains all the partitions that are present
    // in the destination table but not in the source table.
    private final Set<String> remainingPartitions = Sets.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    TableWriter(TableWriterNode tableWriterNode,
            ShardManager shardManager)
    {
        this.tableWriterNode = checkNotNull(tableWriterNode, "tableWriterNode is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");

        this.remainingPartitions.addAll(shardManager.getPartitions(tableWriterNode.getTable()));
    }

    public OutputReceiver getOutputReceiver()
    {
        return new OutputReceiver() {
            @Override
            public void updateOutput(Object result)
            {
                @SuppressWarnings("unchecked")
                TableWriterResult tableWriterResult = TableWriterResult.forMap((Map<String, Object>) result);
                String oldValue = shardsDone.put(tableWriterResult.getShardId(), tableWriterResult.getNodeIdentifier());
                checkState(oldValue == null || oldValue.equals(tableWriterResult.getNodeIdentifier()),
                        "Seen a different node committing a shard (%s vs %s)", oldValue, tableWriterResult.getNodeIdentifier());

                for (Map.Entry<String, Set<Long>> entry : finishedPartitions.entrySet()) {
                    if (!partitionsDone.contains(entry.getKey())) {
                        considerCommittingPartition(entry.getKey(), entry.getValue());
                    }
                }
            }
        };
    }

    private synchronized void considerCommittingPartition(String partitionName, Set<Long> shardIds)
    {
        if (partitionsDone.contains(partitionName)) {
            return; // some other thread raced us here and won. No harm done.
        }

        if (shardsDone.keySet().containsAll(shardIds)) {
            // All shards for this partition have been written. Commit the whole thing.
            ImmutableMap.Builder<Long, String> builder = ImmutableMap.builder();
            for (Long shardId : shardIds) {
                builder.put(shardId, shardsDone.get(shardId));
            }
            shardManager.commitPartition(tableWriterNode.getTable(), partitionName, builder.build());
            checkState(shardsInFlight.addAndGet(-shardIds.size()) >= 0, "shards in flight crashed into the ground");
            partitionsDone.add(partitionName);
        }
    }

    public Iterable<SplitAssignments> getSplitAssignments(Iterable<SplitAssignments> splits)
    {
        return new TableWriterIterable(splits);
    }

    private void addPartitionShard(String partition, boolean lastSplit, Long shardId)
    {
        Set<Long> partitionSplits = openPartitions.get(partition);
        ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        if (partitionSplits != null) {
            builder.addAll(partitionSplits);
        }
        if (shardId != null) {
            builder.add(shardId);
        }
        else {
            checkState(lastSplit, "shardId == null and lastSplit unset!");
        }
        Set<Long> shardIds = builder.build();

        // This can only happen if the method gets called with a partition name, and no shard id and the last split
        // is set. As this only happens to close out the partitions that we saw before (a loop over openPartitions),
        // so any partition showing up here must have at least one split.
        checkState(shardIds.size() > 0, "Never saw a split for partition %s", partition);

        if (lastSplit) {
            checkState(null == finishedPartitions.put(partition, shardIds), "Partition %s finished multiple times", partition);
            openPartitions.remove(partition);
        }
        else {
            openPartitions.put(partition, shardIds);
        }
    }

    private void finishOpenPartitions()
    {
        // commit still open partitions.
        for (String partition : openPartitions.keySet()) {
            addPartitionShard(partition, true, null);
        }

        checkState(openPartitions.size() == 0, "Still open partitions: %s", openPartitions);
    }

    private void dropAdditionalPartitions()
    {
        // drop all the partitions that were not found when scanning through the partitions
        // from the source.
        for (String partition : remainingPartitions) {
            shardManager.dropPartition(tableWriterNode.getTable(), partition);
        }
    }

    public Predicate<PartitionInfo> getPartitionPredicate()
    {
        checkState(!predicateHandedOut.getAndSet(true), "Predicate can only be handed out once");

        final Set<String> allPartitions = ImmutableSet.copyOf(remainingPartitions);

        return new Predicate<PartitionInfo>() {

            public boolean apply(PartitionInfo input)
            {
                remainingPartitions.remove(input.getName());
                return !allPartitions.contains(input.getName());
            }
        };
    }

    private class TableWriterIterable
            implements Iterable<SplitAssignments>
    {
        private final AtomicBoolean used = new AtomicBoolean();
        private final Iterable<SplitAssignments> splits;

        private TableWriterIterable(Iterable<SplitAssignments> splits)
        {
            this.splits = checkNotNull(splits, "splits is null");
        }

        @Override
        public Iterator<SplitAssignments> iterator()
        {
            checkState(!used.getAndSet(true), "The table writer can hand out only a single iterator");
            return new TableWriterIterator(splits.iterator());
        }
    }

    private class TableWriterIterator
            extends AbstractIterator<SplitAssignments>
    {
        private final Iterator<SplitAssignments> sourceIterator;

        private TableWriterIterator(final Iterator<SplitAssignments> sourceIterator)
        {
            this.sourceIterator = sourceIterator;
        }

        @Override
        protected SplitAssignments computeNext()
        {
            if (sourceIterator.hasNext()) {
                SplitAssignments sourceAssignment = sourceIterator.next();
                Map<PlanNodeId, ? extends Split> sourceSplits = sourceAssignment.getSplits();
                checkState(sourceSplits.size() == 1, "Can only augment single table splits");
                Map.Entry<PlanNodeId, ? extends Split> split = Iterables.getOnlyElement(sourceSplits.entrySet());

                NativeSplit writingSplit = new NativeSplit(shardManager.allocateShard(tableWriterNode.getTable()));

                String partition = "unpartitioned";
                boolean lastSplit = false;
                if (split.getValue() instanceof PartitionedSplit) {
                    PartitionedSplit partitionedSplit = (PartitionedSplit) split.getValue();
                    partition = partitionedSplit.getPartition();
                    lastSplit = partitionedSplit.isLastSplit();
                }

                addPartitionShard(partition, lastSplit, writingSplit.getShardId());

                ImmutableMap.Builder<PlanNodeId, Split> builder = ImmutableMap.builder();
                builder.putAll(sourceSplits);
                builder.put(tableWriterNode.getId(), writingSplit);
                Map<PlanNodeId, ? extends Split> newSplits = builder.build();
                shardsInFlight.incrementAndGet();

                return new SplitAssignments(newSplits, sourceAssignment.getNodes());
            }
            else {
                finishOpenPartitions();
                dropAdditionalPartitions();
                return endOfData();
            }
        }
    }
}
