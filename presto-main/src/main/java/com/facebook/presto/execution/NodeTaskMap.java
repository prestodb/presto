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
package com.facebook.presto.execution;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;
import it.unimi.dsi.fastutil.longs.Long2LongRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongSortedMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class NodeTaskMap
{
    private static final Logger log = Logger.get(NodeTaskMap.class);
    private final ConcurrentHashMap<InternalNode, NodeTasks> nodeTasksMap = new ConcurrentHashMap<>();
    private final FinalizerService finalizerService;
    private final long cpuStatsWindowSizeInMillis;

    @Inject
    public NodeTaskMap(FinalizerService finalizerService, TaskManagerConfig taskConfig)
    {
        this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");
        this.cpuStatsWindowSizeInMillis = taskConfig.getStatusRefreshMaxWait().toMillis() * 2;
    }

    public NodeTaskMap(FinalizerService finalizerService, Duration statusRefreshMaxWait)
    {
        this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");
        this.cpuStatsWindowSizeInMillis = requireNonNull(statusRefreshMaxWait, "statusRefreshMaxWait is null").toMillis();
    }

    public void addTask(InternalNode node, RemoteTask task)
    {
        createOrGetNodeTasks(node).addTask(task);
    }

    public int getPartitionedSplitsOnNode(InternalNode node)
    {
        return createOrGetNodeTasks(node).getPartitionedSplitCount();
    }

    public long getNodeTotalMemoryUsageInBytes(InternalNode node)
    {
        return createOrGetNodeTasks(node).getTotalMemoryUsageInBytes();
    }

    public long getNodeCpuUtilizationPercentage(InternalNode node)
    {
        return createOrGetNodeTasks(node).getTotalCpuTimePerMillis();
    }

    public NodeStatsTracker createTaskStatsTracker(InternalNode node, TaskId taskId)
    {
        return createOrGetNodeTasks(node).createTaskStatsTrackers(taskId);
    }

    private NodeTasks createOrGetNodeTasks(InternalNode node)
    {
        return nodeTasksMap.computeIfAbsent(node, key -> new NodeTasks(finalizerService, cpuStatsWindowSizeInMillis));
    }

    private static class NodeTasks
    {
        private final Set<RemoteTask> remoteTasks = Sets.newConcurrentHashSet();
        private final AtomicLong nodeTotalPartitionedSplitCount = new AtomicLong();
        private final AtomicLong nodeTotalMemoryUsageInBytes = new AtomicLong();
        private final AtomicLong nodeTotalCpuTimePerMillis = new AtomicLong();
        private final FinalizerService finalizerService;
        private final long windowSizeInMilis;

        public NodeTasks(FinalizerService finalizerService, long windowSizeInMilis)
        {
            this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");
            this.windowSizeInMilis = windowSizeInMilis;
        }

        private int getPartitionedSplitCount()
        {
            return nodeTotalPartitionedSplitCount.intValue();
        }

        private long getTotalMemoryUsageInBytes()
        {
            return nodeTotalMemoryUsageInBytes.get();
        }

        private long getTotalCpuTimePerMillis()
        {
            return nodeTotalCpuTimePerMillis.get();
        }

        private void addTask(RemoteTask task)
        {
            if (remoteTasks.add(task)) {
                task.addStateChangeListener(taskStatus -> {
                    if (taskStatus.getState().isDone()) {
                        remoteTasks.remove(task);
                    }
                });

                // Check if task state is already done before adding the listener
                if (task.getTaskStatus().getState().isDone()) {
                    remoteTasks.remove(task);
                }
            }
        }

        public NodeStatsTracker createTaskStatsTrackers(TaskId taskId)
        {
            requireNonNull(taskId, "taskId is null");

            TaskStatsTracker splitTracker = new TaskStatsTracker("SplitTracker", taskId, nodeTotalPartitionedSplitCount);
            TaskStatsTracker memoryUsageTracker = new TaskStatsTracker("MemoryTracker", taskId, nodeTotalMemoryUsageInBytes);
            AccumulatedTaskStatsTracker cpuUtilizationPercentageTracker = new AccumulatedTaskStatsTracker("CpuTracker", taskId, nodeTotalCpuTimePerMillis, windowSizeInMilis);
            NodeStatsTracker nodeStatsTracker = new NodeStatsTracker(splitTracker::setValue, memoryUsageTracker::setValue, cpuUtilizationPercentageTracker::setValue);

            // when nodeStatsTracker is garbage collected, run the cleanup method on the tracker
            // Note: tracker can not have a reference to nodeStatsTracker
            finalizerService.addFinalizer(nodeStatsTracker, splitTracker::cleanup);
            finalizerService.addFinalizer(memoryUsageTracker, memoryUsageTracker::cleanup);
            finalizerService.addFinalizer(cpuUtilizationPercentageTracker, cpuUtilizationPercentageTracker::cleanup);

            return nodeStatsTracker;
        }

        @ThreadSafe
        private class TaskStatsTracker
        {
            private final String stat;
            private final TaskId taskId;
            private final AtomicLong totalValue;
            private final AtomicLong value = new AtomicLong();

            public TaskStatsTracker(String stat, TaskId taskId, AtomicLong totalValue)
            {
                this.stat = requireNonNull(stat, "stat is null");
                this.taskId = requireNonNull(taskId, "taskId is null");
                this.totalValue = requireNonNull(totalValue, "totalValue is null");
            }

            public synchronized void setValue(long value)
            {
                if (value < 0) {
                    long oldValue = this.value.getAndSet(0L);
                    totalValue.addAndGet(-oldValue);
                    throw new IllegalArgumentException(stat + " is negative");
                }

                long oldValue = this.value.getAndSet(value);
                totalValue.addAndGet(value - oldValue);
            }

            public void cleanup()
            {
                long leakedValues = value.getAndSet(0);
                if (leakedValues == 0) {
                    return;
                }

                log.error("BUG! %s for %s leaked with %s %s.  Cleaning up so server can continue to function.",
                        getClass().getName(),
                        taskId,
                        leakedValues,
                        stat);

                totalValue.addAndGet(-leakedValues);
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("taskId", taskId)
                        .add(stat, value)
                        .toString();
            }
        }

        // tracks stats which are passed as accumulated (cpu time) by calculating delta / duration.
        @ThreadSafe
        private class AccumulatedTaskStatsTracker
                extends TaskStatsTracker
        {
            private final long windowSizeInMillis;
            private final Long2LongSortedMap values = new Long2LongRBTreeMap();

            AccumulatedTaskStatsTracker(String stat, TaskId taskId, AtomicLong totalValue, long windowSizeInMillis)
            {
                super(stat, taskId, totalValue);
                this.windowSizeInMillis = windowSizeInMillis;
            }

            private long getDeltaPerSecond(long taskAgeInMillis, long value)
            {
                if (value > 0) {
                    values.put(taskAgeInMillis, value);
                    // this clears the map and make items eligible for GC
                    values.headMap(values.lastKey() - windowSizeInMillis).clear();
                    if (values.size() > 1) {
                        long lastLongKey = values.lastLongKey();
                        long firstLongKey = values.firstLongKey();
                        long deltaValue = (values.get(lastLongKey) - values.get(firstLongKey)) * 100;
                        long deltaDuration = lastLongKey - firstLongKey;
                        return deltaDuration >= 0 && deltaValue > 0 ? deltaValue / deltaDuration : 0;
                    }
                }
                return 0;
            }

            public synchronized void setValue(long taskAgeInMillis, long value)
            {
                super.setValue(getDeltaPerSecond(taskAgeInMillis, value));
            }
        }
    }

    public static class NodeStatsTracker
    {
        private final IntConsumer splitSetter;
        private final LongConsumer memoryUsageSetter;
        private final CumulativeStatsConsumer cpuUsageSetter;

        public NodeStatsTracker(IntConsumer splitSetter, LongConsumer memoryUsageSetter, CumulativeStatsConsumer cpuUsageSetter)
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

    public interface CumulativeStatsConsumer
    {
        void accept(long age, long value);
    }
}
