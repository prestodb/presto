package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.operator.TableWriterResult;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.split.PartitionedSplit;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.split.WritingSplit;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;

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
    private static final Logger log = Logger.get(TableWriter.class);

    private final TableWriterNode node;
    private final ShardManager shardManager;

    // Which shards are part of which partition
    private final Map<String, Set<Long>> openPartitions = new ConcurrentHashMap<>();
    private final Map<String, Set<Long>> finishedPartitions = new ConcurrentHashMap<>();

    // Which shards have already been written to disk and where.
    private final Map<Long, String> shardsDone = new ConcurrentHashMap<Long, String>();

    // Which partitions have already been committed.
    private final Set<String> partitionsDone = Sets.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    private final AtomicInteger shardsInFlight = new AtomicInteger();

    TableWriter(TableWriterNode node,
            ShardManager shardManager)
    {
        this.node = checkNotNull(node, "node is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
    }

    public OutputReceiver getOutputReceiver()
    {
        return new OutputReceiver() {
            @Override
            public void receive(Object result)
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
            shardManager.commitPartition(node.getTableHandle(), partitionName, builder.build());
            checkState(shardsInFlight.addAndGet(-shardIds.size()) >= 0, "shards in flight crashed into the ground");
            partitionsDone.add(partitionName);
            log.info("Partition %s is now committed, seen %d splits", partitionName, shardIds.size());
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
        Set<Long> shardIds = builder.build();
        checkState(shardIds.size() > 0, "Never saw a split for partition %s", partition);

        if (lastSplit) {
            checkState(null == finishedPartitions.put(partition, shardIds), "Partition %s finished multiple times", partition);
            openPartitions.remove(partition);
            log.info("Partition %s is now fully scheduled, seen %d splits, Partitions stats: FLIGHT: %d, READY: %d, COMMIT: %d)", partition, shardIds.size(), openPartitions.size(), finishedPartitions.size(), partitionsDone.size());
        }
        else {
            openPartitions.put(partition, shardIds);
        }
    }

    public Predicate<PartitionInfo> getPartitionPredicate()
    {
        final Set<String> existingPartitions = ImmutableSet.copyOf(shardManager.getPartitions(node.getTableHandle()));

        return new Predicate<PartitionInfo>() {

            public boolean apply(PartitionInfo input)
            {
                return !existingPartitions.contains(input.getName());
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

                WritingSplit writingSplit = new WritingSplit(shardManager.allocateShard(node.getTableHandle()));

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
                builder.put(node.getId(), writingSplit);
                Map<PlanNodeId, ? extends Split> newSplits = builder.build();
                log.info("Scheduling Shard for %s, now %d shards in flight", partition, shardsInFlight.incrementAndGet());

                return new SplitAssignments(newSplits, sourceAssignment.getNodes());
            }
            else {
                for (String partition : openPartitions.keySet()) {
                    addPartitionShard(partition, true, null);
                }
                checkState(openPartitions.size() == 0, "Still open partitions: %s", openPartitions);
                return endOfData();
            }
        }
    }
}
