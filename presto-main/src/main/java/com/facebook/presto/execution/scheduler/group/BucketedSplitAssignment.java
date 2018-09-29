package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;

import java.util.Optional;
import java.util.function.ToIntFunction;

public abstract class BucketedSplitAssignment
{
    private final ToIntFunction<Split> splitToBucket;

    public BucketedSplitAssignment(ToIntFunction<Split> splitToBucket)
    {
        this.splitToBucket = splitToBucket;
    }

    public abstract Optional<Node> getAssignedNode(int bucketedId);

    public abstract void assignOrChangeBucketToNode(int bucketedId, Node node);

    public final Optional<Node> getAssignedNode(Split split)
    {
        return getAssignedNode(splitToBucket.applyAsInt(split));
    }
}