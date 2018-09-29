package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;

import java.util.Optional;
import java.util.function.ToIntFunction;

// buckets are pre-allocated to nodes, derived from NodePartitioningMap
public class StaticBucketedSplitAssignment
        extends BucketedSplitAssignment
{
    private final Node[] bucketToNode;

    public StaticBucketedSplitAssignment(ToIntFunction<Split> splitToBucket, Node[] bucketToNode)
    {
        super(splitToBucket);
        this.bucketToNode = bucketToNode;
    }

    @Override
    public Optional<Node> getAssignedNode(int bucketedId)
    {
        return Optional.of(bucketToNode[bucketedId]);
    }

    @Override
    public void assignOrChangeBucketToNode(int bucketedId, Node node)
    {
        throw new UnsupportedOperationException();
    }
}