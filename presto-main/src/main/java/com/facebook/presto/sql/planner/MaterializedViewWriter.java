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
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.operator.MaterializedViewWriterResult;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.PartitionedSplit;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.CollocatedSplit;
import com.facebook.presto.split.NativeSplit;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.MaterializedViewWriterNode;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.metadata.TablePartition.partitionNameGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Collections2.transform;

public class MaterializedViewWriter
{
    private final MaterializedViewWriterNode writerNode;
    private final ShardManager shardManager;

    // Which shards are part of which partition
    private final Map<String, PartitionInfo> openPartitions = new ConcurrentHashMap<>();
    private final Map<String, PartitionInfo> finishedPartitions = new ConcurrentHashMap<>();

    // Which shards have already been written to disk and where.
    private final Map<Long, String> shardsDone = new ConcurrentHashMap<>();

    // Which partitions have already been committed.
    private final Set<String> partitionsDone = Sets.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    private final AtomicInteger shardsInFlight = new AtomicInteger();

    private final AtomicBoolean predicateHandedOut = new AtomicBoolean();

    // After finishing iteration over all source partitions, this set contains all the partitions that are present
    // in the destination table but not in the source table.
    private final Set<String> remainingPartitions = Sets.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    MaterializedViewWriter(MaterializedViewWriterNode writerNode, ShardManager shardManager)
    {
        this.writerNode = checkNotNull(writerNode, "writerNode is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");

        this.remainingPartitions.addAll(transform(shardManager.getPartitions(writerNode.getTable()), partitionNameGetter()));
    }

    public OutputReceiver getOutputReceiver()
    {
        return new OutputReceiver()
        {
            @Override
            public void updateOutput(Object result)
            {
                @SuppressWarnings("unchecked")
                MaterializedViewWriterResult writerResult = MaterializedViewWriterResult.forMap((Map<String, Object>) result);
                String oldValue = shardsDone.put(writerResult.getShardId(), writerResult.getNodeIdentifier());
                checkState(oldValue == null || oldValue.equals(writerResult.getNodeIdentifier()),
                        "Seen a different node committing a shard (%s vs %s)", oldValue, writerResult.getNodeIdentifier());

                for (Map.Entry<String, PartitionInfo> entry : finishedPartitions.entrySet()) {
                    if (!partitionsDone.contains(entry.getKey())) {
                        considerCommittingPartition(entry.getKey(), entry.getValue());
                    }
                }
            }
        };
    }

    private synchronized void considerCommittingPartition(String partitionName, PartitionInfo partitionInfo)
    {
        if (partitionsDone.contains(partitionName)) {
            return; // some other thread raced us here and won. No harm done.
        }

        Set<Long> shardIds = partitionInfo.getShardIds();
        if (shardsDone.keySet().containsAll(shardIds)) {
            // All shards for this partition have been written. Commit the whole thing.
            ImmutableMap.Builder<Long, String> builder = ImmutableMap.builder();
            for (Long shardId : shardIds) {
                builder.put(shardId, shardsDone.get(shardId));
            }
            shardManager.commitPartition(writerNode.getTable(), partitionName, partitionInfo.getPartitionKeys(), builder.build());
            checkState(shardsInFlight.addAndGet(-shardIds.size()) >= 0, "shards in flight crashed into the ground");
            partitionsDone.add(partitionName);
        }
    }

    public Iterable<Split> wrapSplits(PlanNodeId planNodeId, Iterable<Split> splits)
    {
        return new WriterIterable(planNodeId, splits);
    }

    private void addPartitionShard(String partition, boolean lastSplit, List<? extends PartitionKey> partitionKeys, Long shardId)
    {
        PartitionInfo partitionInfo = openPartitions.get(partition);

        ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        if (partitionInfo != null) {
            Set<Long> partitionSplits = partitionInfo.getShardIds();
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
        checkState(!shardIds.isEmpty(), "Never saw a split for partition %s", partition);

        PartitionInfo newPartitionInfo = new PartitionInfo(shardIds, partitionKeys);
        if (lastSplit) {
            checkState(null == finishedPartitions.put(partition, newPartitionInfo), "Partition %s finished multiple times", partition);
            openPartitions.remove(partition);
        }
        else {
            openPartitions.put(partition, newPartitionInfo);
        }
    }

    private void finishOpenPartitions()
    {
        // commit still open partitions.
        for (String partition : openPartitions.keySet()) {
            addPartitionShard(partition, true, ImmutableList.<PartitionKey>of(), null);
        }

        checkState(openPartitions.isEmpty(), "Still open partitions: %s", openPartitions);
    }

    private void dropAdditionalPartitions()
    {
        // drop all the partitions that were not found when scanning through the partitions
        // from the source.
        for (String partition : remainingPartitions) {
            shardManager.dropPartition(writerNode.getTable(), partition);
        }
    }

    public Predicate<Partition> getPartitionPredicate()
    {
        checkState(!predicateHandedOut.getAndSet(true), "Predicate can only be handed out once");

        final Set<String> allPartitions = ImmutableSet.copyOf(remainingPartitions);

        return new Predicate<Partition>()
        {
            @Override
            public boolean apply(Partition input)
            {
                remainingPartitions.remove(input.getPartitionId());
                return !allPartitions.contains(input.getPartitionId());
            }
        };
    }

    private class WriterIterable
            implements Iterable<Split>
    {
        private final AtomicBoolean used = new AtomicBoolean();
        private final Iterable<Split> splits;
        private final PlanNodeId planNodeId;

        private WriterIterable(PlanNodeId planNodeId, Iterable<Split> splits)
        {
            this.planNodeId = checkNotNull(planNodeId, "planNodeId is null");
            this.splits = checkNotNull(splits, "splits is null");
        }

        @Override
        public Iterator<Split> iterator()
        {
            checkState(!used.getAndSet(true), "The table writer can hand out only a single iterator");
            return new WriterIterator(planNodeId, splits.iterator());
        }
    }

    private class WriterIterator
            extends AbstractIterator<Split>
    {
        private final PlanNodeId planNodeId;
        private final Iterator<Split> sourceIterator;

        private WriterIterator(PlanNodeId planNodeId, Iterator<Split> sourceIterator)
        {
            this.planNodeId = planNodeId;
            this.sourceIterator = sourceIterator;
        }

        @Override
        protected Split computeNext()
        {
            if (sourceIterator.hasNext()) {
                Split sourceSplit = sourceIterator.next();

                NativeSplit writingSplit = new NativeSplit(shardManager.allocateShard(writerNode.getTable()), ImmutableList.<HostAddress>of());

                String partition = "unpartitioned";
                boolean lastSplit = false;
                List<? extends PartitionKey> partitionKeys = ImmutableList.of();

                if (sourceSplit instanceof PartitionedSplit) {
                    PartitionedSplit partitionedSplit = (PartitionedSplit) sourceSplit;
                    partition = partitionedSplit.getPartitionId();
                    lastSplit = partitionedSplit.isLastSplit();
                    partitionKeys = partitionedSplit.getPartitionKeys();
                }

                addPartitionShard(partition, lastSplit, partitionKeys, writingSplit.getShardId());
                CollocatedSplit collocatedSplit = new CollocatedSplit(
                        ImmutableMap.of(
                                planNodeId, sourceSplit,
                                writerNode.getId(), writingSplit),
                        sourceSplit.getAddresses(),
                        sourceSplit.isRemotelyAccessible());

                shardsInFlight.incrementAndGet();

                return collocatedSplit;
            }
            finishOpenPartitions();
            dropAdditionalPartitions();
            return endOfData();
        }
    }

    private static final class PartitionInfo
    {
        private final Set<Long> shardIds;
        private final List<? extends PartitionKey> partitionKeys;

        private PartitionInfo(Set<Long> shardIds, List<? extends PartitionKey> partitionKeys)
        {
            this.shardIds = shardIds;
            this.partitionKeys = partitionKeys;
        }

        public Set<Long> getShardIds()
        {
            return shardIds;
        }

        public List<? extends PartitionKey> getPartitionKeys()
        {
            return partitionKeys;
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("shardIds", shardIds)
                    .add("partitionKeys", partitionKeys)
                    .toString();
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(shardIds, partitionKeys);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PartitionInfo other = (PartitionInfo) obj;
            return Objects.equal(this.shardIds, other.shardIds) &&
                    Objects.equal(this.partitionKeys, other.partitionKeys);
        }
    }
}
