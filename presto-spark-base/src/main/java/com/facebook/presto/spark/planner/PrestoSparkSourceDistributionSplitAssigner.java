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
package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.SplitSource;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMaxSparkInputPartitionCountForAutoTune;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMaxSplitsDataSizePerSparkPartition;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMinSparkInputPartitionCountForAutoTune;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getSparkInitialPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getSplitAssignmentBatchSize;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isSparkPartitionCountAutoTuneEnabled;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class PrestoSparkSourceDistributionSplitAssigner
        implements PrestoSparkSplitAssigner
{
    private final PlanNodeId tableScanNodeId;
    private final SplitSource splitSource;

    private final int maxBatchSize;
    private final long maxSplitsSizePerPartitionInBytes;
    private final int initialPartitionCount;
    private final boolean partitionCountAutoTuneEnabled;
    private final int minSparkInputPartitionCountForAutoTune;
    private final int maxSparkInputPartitionCountForAutoTune;

    private final PriorityQueue<Partition> queue = new PriorityQueue<>();
    private int sequenceId;
    private int partitionCount;

    public static PrestoSparkSourceDistributionSplitAssigner create(Session session, PlanNodeId tableScanNodeId, SplitSource splitSource)
    {
        return new PrestoSparkSourceDistributionSplitAssigner(
                tableScanNodeId,
                splitSource,
                getSplitAssignmentBatchSize(session),
                getMaxSplitsDataSizePerSparkPartition(session).toBytes(),
                getSparkInitialPartitionCount(session),
                isSparkPartitionCountAutoTuneEnabled(session),
                getMinSparkInputPartitionCountForAutoTune(session),
                getMaxSparkInputPartitionCountForAutoTune(session));
    }

    public PrestoSparkSourceDistributionSplitAssigner(
            PlanNodeId tableScanNodeId,
            SplitSource splitSource,
            int maxBatchSize,
            long maxSplitsSizePerPartitionInBytes,
            int initialPartitionCount,
            boolean partitionCountAutoTuneEnabled,
            int minSparkInputPartitionCountForAutoTune,
            int maxSparkInputPartitionCountForAutoTune)
    {
        this.tableScanNodeId = requireNonNull(tableScanNodeId, "tableScanNodeId is null");
        this.splitSource = requireNonNull(splitSource, "splitSource is null");
        this.maxBatchSize = maxBatchSize;
        checkArgument(maxBatchSize > 0, "maxBatchSize must be greater than zero");
        this.maxSplitsSizePerPartitionInBytes = maxSplitsSizePerPartitionInBytes;
        checkArgument(maxSplitsSizePerPartitionInBytes > 0,
                "maxSplitsSizePerPartitionInBytes must be greater than zero: %s", maxSplitsSizePerPartitionInBytes);
        this.initialPartitionCount = initialPartitionCount;
        checkArgument(initialPartitionCount > 0,
                "initialPartitionCount must be greater then zero: %s", initialPartitionCount);
        this.partitionCountAutoTuneEnabled = partitionCountAutoTuneEnabled;
        this.minSparkInputPartitionCountForAutoTune = minSparkInputPartitionCountForAutoTune;
        this.maxSparkInputPartitionCountForAutoTune = maxSparkInputPartitionCountForAutoTune;
        checkArgument(minSparkInputPartitionCountForAutoTune >= 1 && minSparkInputPartitionCountForAutoTune <= maxSparkInputPartitionCountForAutoTune,
                "Min partition count for auto tune (%s) should be a positive integer and not larger than max partition count (%s)",
                minSparkInputPartitionCountForAutoTune,
                maxSparkInputPartitionCountForAutoTune);
    }

    @Override
    public Optional<SetMultimap<Integer, ScheduledSplit>> getNextBatch()
    {
        if (splitSource.isFinished()) {
            return Optional.empty();
        }

        List<ScheduledSplit> scheduledSplits = new ArrayList<>();
        while (true) {
            int remaining = maxBatchSize - scheduledSplits.size();
            if (remaining <= 0) {
                break;
            }
            SplitSource.SplitBatch splitBatch = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), min(remaining, 1000)));
            for (Split split : splitBatch.getSplits()) {
                scheduledSplits.add(new ScheduledSplit(sequenceId++, tableScanNodeId, split));
            }
            if (splitBatch.isLastBatch() || splitSource.isFinished()) {
                break;
            }
        }

        return Optional.of(assignSplitsToTasks(scheduledSplits));
    }

    private SetMultimap<Integer, ScheduledSplit> assignSplitsToTasks(List<ScheduledSplit> splits)
    {
        // expected to be mutable for efficiency reasons
        HashMultimap<Integer, ScheduledSplit> result = HashMultimap.create();

        boolean splitsDataSizeAvailable = splits.stream()
                .allMatch(split -> split.getSplit().getConnectorSplit().getSplitSizeInBytes().isPresent());
        if (!splitsDataSizeAvailable) {
            for (int splitIndex = 0; splitIndex < splits.size(); splitIndex++) {
                result.put(splitIndex % initialPartitionCount, splits.get(splitIndex));
            }
            return result;
        }

        splits.sort((ScheduledSplit o1, ScheduledSplit o2) -> {
            long size1 = o1.getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong();
            long size2 = o2.getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong();
            return Long.compare(size2, size1);
        });

        if (partitionCountAutoTuneEnabled) {
            for (int splitIndex = 0; splitIndex < splits.size(); splitIndex++) {
                int partitionId;
                long splitSizeInBytes = splits.get(splitIndex).getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong();

                if ((partitionCount >= minSparkInputPartitionCountForAutoTune &&
                        queue.peek().getSplitsInBytes() + splitSizeInBytes <= maxSplitsSizePerPartitionInBytes)
                        || partitionCount == maxSparkInputPartitionCountForAutoTune) {
                    Partition partition = queue.poll();
                    partitionId = partition.getPartitionId();
                    partition.assignSplitWithSize(splitSizeInBytes);
                    queue.add(partition);
                }
                else {
                    partitionId = partitionCount++;
                    Partition newPartition = new Partition(partitionId);
                    newPartition.assignSplitWithSize(splitSizeInBytes);
                    queue.add(newPartition);
                }

                result.put(partitionId, splits.get(splitIndex));
            }
        }
        else {
            // partition count is fixed
            for (int splitIndex = 0; splitIndex < splits.size(); splitIndex++) {
                int partitionId;
                long splitSizeInBytes = splits.get(splitIndex).getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong();

                if (partitionCount < initialPartitionCount) {
                    partitionId = partitionCount++;
                    Partition newPartition = new Partition(partitionId);
                    newPartition.assignSplitWithSize(splitSizeInBytes);
                    queue.add(newPartition);
                }
                else {
                    Partition partition = queue.poll();
                    partitionId = partition.getPartitionId();
                    partition.assignSplitWithSize(splitSizeInBytes);
                    queue.add(partition);
                }

                result.put(partitionId, splits.get(splitIndex));
            }
        }

        return result;
    }

    @Override
    public void close()
    {
        splitSource.close();
    }

    private static class Partition
            implements Comparable<Partition>
    {
        private final int partitionId;
        private long splitsInBytes;

        public Partition(int partitionId)
        {
            this.partitionId = partitionId;
        }

        public int getPartitionId()
        {
            return partitionId;
        }

        public void assignSplitWithSize(long splitSizeInBytes)
        {
            splitsInBytes += splitSizeInBytes;
        }

        public long getSplitsInBytes()
        {
            return splitsInBytes;
        }

        @Override
        public int compareTo(Partition o)
        {
            // always prefer partition with a lower partition id to make split assignment result deterministic
            return ComparisonChain.start()
                    .compare(splitsInBytes, o.splitsInBytes)
                    .compare(partitionId, o.partitionId)
                    .result();
        }
    }
}
