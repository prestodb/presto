package com.facebook.presto.execution;

import com.facebook.presto.metadata.InternalNode;

import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public interface NodeTaskTracker
{
    void addTask(InternalNode node, RemoteTask task);

    int getPartitionedSplitsOnNode(InternalNode node);

    long getNodeTotalMemoryUsageInBytes(InternalNode node);

    double getNodeCpuUtilizationPercentage(InternalNode node);

    NodeTaskMap.NodeStatsTracker createTaskStatsTracker(InternalNode node, TaskId taskId);

    class NodeStatsTracker
    {
        private final IntConsumer splitSetter;
        private final LongConsumer memoryUsageSetter;
        private final NodeTaskMap.CumulativeStatsConsumer cpuUsageSetter;

        public NodeStatsTracker(IntConsumer splitSetter, LongConsumer memoryUsageSetter, NodeTaskMap.CumulativeStatsConsumer cpuUsageSetter)
        {
            this.splitSetter = requireNonNull(splitSetter, "splitSetter is null");
            this.memoryUsageSetter = requireNonNull(memoryUsageSetter, "memoryUsageSetter is null");
            this.cpuUsageSetter = requireNonNull(cpuUsageSetter, "cpuUsageSetter is null");
        }

        public void setPartitionedSplitCount(int partitionedSplitCount)
        {
            splitSetter.accept(partitionedSplitCount);
        }

        public void setMemoryUsage(long memoryUsage)
        {
            memoryUsageSetter.accept(memoryUsage);
        }

        public void setCpuUsage(long age, long cpuUsage)
        {
            cpuUsageSetter.accept(age, cpuUsage);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("splitSetter", splitSetter.toString())
                    .add("memoryUsageSetter", memoryUsageSetter.toString())
                    .add("cpuUsageSetter", cpuUsageSetter.toString())
                    .toString();
        }
    }
}
