package com.facebook.presto.execution;

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.resourcemanager.ClusterNodeManagerService;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class RemoteNodeTaskTracker
        implements NodeTaskTracker
{
    private final ClusterNodeManagerService nodeManagerService;

    @GuardedBy("this")
    private final Map<String, Set<TaskId>> nodes = new HashMap<>();

    @Inject
    public RemoteNodeTaskTracker(ClusterNodeManagerService nodeManagerService)
    {
        this.nodeManagerService = requireNonNull(nodeManagerService, "nodeManagerService is null");
    }

    @Override
    public synchronized void addTask(InternalNode node, RemoteTask task)
    {
        Set<TaskId> taskIds = nodes.computeIfAbsent(node.getNodeIdentifier(), internalNode -> new HashSet<>());
        if (taskIds.add(task.getTaskId())) {
            task.addStateChangeListener(newState -> {
                if (newState.getState().isDone()) {
                    removeEmptyNode(task);
                }
            });
        }
    }

    private synchronized void removeEmptyNode(RemoteTask task)
    {
        Set<TaskId> taskIds = nodes.get(task.getNodeId());
        taskIds.remove(task.getTaskId());
        if (taskIds.isEmpty()) {
            nodes.remove(task.getNodeId());
        }
    }

    @Override
    public synchronized int getPartitionedSplitsOnNode(InternalNode node)
    {
        if (!nodes.containsKey(node.getNodeIdentifier())) {
            return 0;
        }
        return nodeManagerService.getNodes().get(node.getNodeIdentifier()).getNodeSplitStats().getTotalPartitionedSplitCount();
    }

    @Override
    public synchronized long getNodeTotalMemoryUsageInBytes(InternalNode node)
    {
        if (!nodes.containsKey(node.getNodeIdentifier())) {
            return 0;
        }
        return nodeManagerService.getNodes().get(node.getNodeIdentifier()).getHeapUsed();
    }

    @Override
    public synchronized double getNodeCpuUtilizationPercentage(InternalNode node)
    {
        if (!nodes.containsKey(node.getNodeIdentifier())) {
            return 0;
        }
        return nodeManagerService.getNodes().get(node.getNodeIdentifier()).getProcessCpuLoad();
    }

    @Override
    public NodeStatsTracker createTaskStatsTracker(InternalNode node, TaskId taskId)
    {
        return new NodeStatsTracker(value -> {}, value -> {}, (age, value) -> {});
    }
}
