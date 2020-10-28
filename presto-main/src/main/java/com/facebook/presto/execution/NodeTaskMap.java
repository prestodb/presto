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
import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.stats.ExponentialDecay;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.Sets;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@ThreadSafe
public class NodeTaskMap
{
    private static final Logger log = Logger.get(NodeTaskMap.class);
    private final ConcurrentHashMap<InternalNode, NodeTasks> nodeTasksMap = new ConcurrentHashMap<>();
    private final FinalizerService finalizerService;
    private final ScheduledExecutorService logger = newScheduledThreadPool(1, daemonThreadsNamed("node-task-map"));
    private Distribution nodeCpuDistribution = new Distribution(ExponentialDecay.seconds(10));
    private Distribution nodeMemoryDistribution = new Distribution(ExponentialDecay.seconds(10));

    @Inject
    public NodeTaskMap(FinalizerService finalizerService)
    {
        this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");

        logger.scheduleAtFixedRate(() -> {
            if (!nodeTasksMap.isEmpty()) {
                log.info("------------------");
                Distribution nodeCpuDistribution = new Distribution(ExponentialDecay.seconds(10));
                Distribution nodeMemoryDistribution = new Distribution(ExponentialDecay.seconds(10));
                boolean printOneTime = true;
                for (Map.Entry<InternalNode, NodeTasks> internalNodeNodeTasksEntry : nodeTasksMap.entrySet()) {
                    if (printOneTime) {
                        log.info(
                                "Node: " + internalNodeNodeTasksEntry.getKey().getNodeIdentifier() +
                                        " Cpu: " + internalNodeNodeTasksEntry.getValue().nodeTotalCpuUsage + "/" + internalNodeNodeTasksEntry.getKey().getNodeCpuCore().getAsInt() +
                                        " Mem: " + internalNodeNodeTasksEntry.getValue().nodeTotalMemoryUsage + "/" + internalNodeNodeTasksEntry.getKey().getNodeMemory().getAsLong() +
                                        " Split: " + internalNodeNodeTasksEntry.getValue().nodeTotalPartitionedSplitCount);

                        nodeCpuDistribution.add(internalNodeNodeTasksEntry.getValue().nodeTotalCpuUsage.longValue());
                        nodeMemoryDistribution.add(internalNodeNodeTasksEntry.getValue().nodeTotalMemoryUsage.longValue());
                        Set<RemoteTask> remoteTasks = internalNodeNodeTasksEntry.getValue().remoteTasks;
                    }
                    printOneTime = false;
                }
//                log.info(nodeCpuDistribution.snapshot().toString());
//                log.info(nodeMemoryDistribution.snapshot().toString());
                this.nodeCpuDistribution = nodeCpuDistribution;
                this.nodeMemoryDistribution = nodeMemoryDistribution;
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public Distribution getNodeCpuDistribution()
    {
        return nodeCpuDistribution;
    }

    public Distribution getNodeMemoryDistribution()
    {
        return nodeMemoryDistribution;
    }

    public void addTask(InternalNode node, RemoteTask task)
    {
        createOrGetNodeTasks(node).addTask(task);
    }

    public int getPartitionedSplitsOnNode(InternalNode node)
    {
        return createOrGetNodeTasks(node).getPartitionedSplitCount();
    }

    public long getMemoryUsageOnNode(InternalNode node)
    {
        return createOrGetNodeTasks(node).getMemoryUsage();
    }

    public long getCpuUsageOnNode(InternalNode node)
    {
        return createOrGetNodeTasks(node).getCpuUsage();
    }

    public NodeStatsTracker createNodeStatsTracker(InternalNode node, TaskId taskId)
    {
        return createOrGetNodeTasks(node).createTaskStatsTracker(taskId);
    }

    private NodeTasks createOrGetNodeTasks(InternalNode node)
    {
        NodeTasks nodeTasks = nodeTasksMap.get(node);
        if (nodeTasks == null) {
            nodeTasks = addNodeTask(node);
        }
        return nodeTasks;
    }

    private NodeTasks addNodeTask(InternalNode node)
    {
        NodeTasks newNodeTasks = new NodeTasks(finalizerService);
        NodeTasks nodeTasks = nodeTasksMap.putIfAbsent(node, newNodeTasks);
        if (nodeTasks == null) {
            return newNodeTasks;
        }
        return nodeTasks;
    }

    private static class NodeTasks
    {
        private final Set<RemoteTask> remoteTasks = Sets.newConcurrentHashSet();
        private final AtomicLong nodeTotalPartitionedSplitCount = new AtomicLong();
        private final AtomicLong nodeTotalMemoryUsage = new AtomicLong();
        private final AtomicLong nodeTotalCpuUsage = new AtomicLong();
        private final FinalizerService finalizerService;

        public NodeTasks(FinalizerService finalizerService)
        {
            this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");
        }

        private int getPartitionedSplitCount()
        {
            return (int) nodeTotalPartitionedSplitCount.get();
        }

        private long getMemoryUsage()
        {
            return nodeTotalMemoryUsage.get();
        }

        private long getCpuUsage()
        {
            return nodeTotalCpuUsage.get();
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

        public NodeStatsTracker createTaskStatsTracker(TaskId taskId)
        {
            requireNonNull(taskId, "taskId is null");

            TaskResourceTracker splitTracker = new TaskResourceTracker(taskId, "SplitTracker", nodeTotalPartitionedSplitCount);
            TaskResourceTracker memoryUsageTracker = new TaskResourceTracker(taskId, "MemoryTracker", nodeTotalMemoryUsage);
            TaskCumulativeResourceTracker cpuUsageTracker = new TaskCumulativeResourceTracker(taskId, "CpuTracker", nodeTotalCpuUsage);
            NodeStatsTracker nodeStatsTracker = new NodeStatsTracker(splitTracker::setResource, memoryUsageTracker::setResource, cpuUsageTracker::setCumulativeResource);

            // when nodeStatsTracker is garbage collected, run the cleanup method on the tracker
            // Note: tracker can not have a reference to nodeStatsTracker
            finalizerService.addFinalizer(splitTracker, splitTracker::cleanup);
            finalizerService.addFinalizer(memoryUsageTracker, memoryUsageTracker::cleanup);

            return nodeStatsTracker;
        }

        @ThreadSafe
        private class TaskResourceTracker
        {
            private final TaskId taskId;
            private final String resourceName;
            private final AtomicLong totalResource;
            private final AtomicLong localResource = new AtomicLong();

            public TaskResourceTracker(TaskId taskId, String resourceName, AtomicLong totalResource)
            {
                this.taskId = requireNonNull(taskId, "taskId is null");
                this.resourceName = requireNonNull(resourceName, "resourceName is null");
                this.totalResource = requireNonNull(totalResource, "totalResource is null");
            }

            public synchronized void setResource(long resource)
            {
                if (resource < 0) {
                    long oldValue = localResource.getAndSet(0);
                    totalResource.addAndGet(-oldValue);
                    throw new IllegalArgumentException("resource is negative");
                }

                long oldValue = localResource.getAndSet(resource);
                totalResource.addAndGet(resource - oldValue);
            }

            public void cleanup()
            {
                long leakedResources = localResource.getAndSet(0);
                if (leakedResources == 0) {
                    return;
                }

                log.error("BUG! %s for %s leaked with %s %s.  Cleaning up so server can continue to function.",
                        getClass().getName(),
                        taskId,
                        leakedResources,
                        resourceName);

                totalResource.addAndGet(-leakedResources);
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("taskId", taskId)
                        .add(resourceName, localResource)
                        .toString();
            }
        }

        @ThreadSafe
        private class TaskCumulativeResourceTracker
                extends TaskResourceTracker
        {
            SortedMap<Long, Long> resource = new TreeMap<>();

            public TaskCumulativeResourceTracker(TaskId taskId, String resourceName, AtomicLong totalResource)
            {
                super(taskId, resourceName, totalResource);
            }

            private long getDeltaPerSecond(long taskAge, long cumulativeResource)
            {
                if (cumulativeResource > 0) {
                    resource.put(taskAge, cumulativeResource);
                    resource = resource.tailMap(resource.lastKey() - 2_000_000_000);
                    if (resource.size() > 1) {
                        Long firstReporting = resource.firstKey();
                        Long lastAge = resource.lastKey();
                        long delta = (cumulativeResource - resource.get(firstReporting)) * 100;
                        long duration = lastAge - firstReporting;
                        if (delta >= 0 && duration > 0) {
                            return delta / duration;
                        }
                        else {
                            System.out.println("Delta is negative");
                        }
                    }
                }
                return 0;
            }

            public synchronized void setCumulativeResource(long taskAge, long cumulativeResource)
            {
                super.setResource(getDeltaPerSecond(taskAge, cumulativeResource));
            }
        }
    }

    public static class NodeStatsTracker
    {
        private final IntConsumer splitSetter;
        private final LongConsumer memoryUsageSetter;
        private final CumulativeResourceConsumer cpuUsageSetter;

        public NodeStatsTracker(IntConsumer splitSetter, LongConsumer memoryUsageSetter, CumulativeResourceConsumer cpuUsageSetter)
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
                    .add("splitSetter", splitSetter)
                    .add("memoryUsageSetter", memoryUsageSetter)
                    .toString();
        }
    }

    public interface CumulativeResourceConsumer
    {
        void accept(long age, long value);
    }
}
