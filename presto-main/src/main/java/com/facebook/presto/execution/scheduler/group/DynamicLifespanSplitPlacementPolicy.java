package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.NodeSelector;
import com.facebook.presto.execution.scheduler.SplitPlacementPolicy;
import com.facebook.presto.execution.scheduler.SplitPlacementResult;
import com.facebook.presto.spi.Node;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicLifespanSplitPlacementPolicy
        implements SplitPlacementPolicy
{
    private final NodeSelector nodeSelector;
    private final List<Node> nodeList;
    private final BucketedSplitAssignment bucketedSplitAssignment;
    private final Supplier<? extends List<RemoteTask>> remoteTasks;

    public DynamicLifespanSplitPlacementPolicy(
            NodeSelector nodeSelector,
            List<Node> nodeList,
            BucketedSplitAssignment bucketedSplitAssignment,
            Supplier<? extends List<RemoteTask>> remoteTasks)
    {
        this.nodeSelector = requireNonNull(nodeSelector, "nodeSelector is null");
        this.nodeList = requireNonNull(nodeList, "nodeList is null");
        this.bucketedSplitAssignment = requireNonNull(bucketedSplitAssignment, "bucketedSplitAssignment is null");
        this.remoteTasks = requireNonNull(remoteTasks, "remoteTasks is null");
    }

    @Override
    public SplitPlacementResult computeAssignments(SplitPlacementSet splits)
    {
        return nodeSelector.computeAssignments(splits.getSplits(), remoteTasks.get(), bucketedSplitAssignment);
    }

    @Override
    public void lockDownNodes()
    {
    }

    @Override
    public List<Node> allNodes()
    {
        return ImmutableList.copyOf(nodeList);
    }

    @Override
    public Node getNodeForLifespan(Lifespan lifespan)
    {
        checkArgument(!lifespan.isTaskWide());
        return bucketedSplitAssignment.getAssignedNode(lifespan.getId()).get();
    }
}