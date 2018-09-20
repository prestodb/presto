package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.NodeSelector;
import com.facebook.presto.execution.scheduler.SplitPlacementPolicy;
import com.facebook.presto.execution.scheduler.SplitPlacementResult;
import com.facebook.presto.spi.Node;

import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// only feasible for co-located join + group execution, since there is no remote source
// buckets are assigned in a dynamic way, but the splits in the same bucket will be assigned to the same node
public class DynamicBucketAwareSplitPlacementPolicy
        implements SplitPlacementPolicy
{
    private final NodeSelector nodeSelector;
    private final BucketedSplitAssignment bucketedSplitAssignment;
    private final Supplier<? extends List<RemoteTask>> remoteTasks;

    public DynamicBucketAwareSplitPlacementPolicy(
            NodeSelector nodeSelector,
            BucketedSplitAssignment bucketedSplitAssignment,
            Supplier<? extends List<RemoteTask>> remoteTasks)
    {
        this.nodeSelector = requireNonNull(nodeSelector, "nodeSelector is null");
        this.bucketedSplitAssignment = requireNonNull(bucketedSplitAssignment, "bucketedSplitAssignment is null");
        this.remoteTasks = requireNonNull(remoteTasks, "remoteTasks is null");
    }

    @Override
    public SplitPlacementResult computeAssignments(SplitPlacementSet splits)
    {
        checkArgument(!splits.getLifespan().isTaskWide());
        return nodeSelector.computeAssignments(splits, remoteTasks.get(), bucketedSplitAssignment);
    }

    @Override
    public void lockDownNodes()
    {
    }

    @Override
    public List<Node> allNodes()
    {
        return nodeSelector.allNodes();
    }

    @Override
    public Node getNodeForLifespan(Lifespan lifespan)
    {
        checkArgument(!lifespan.isTaskWide());
        return bucketedSplitAssignment.getAssignedNode(lifespan.getId()).get();
    }
}
