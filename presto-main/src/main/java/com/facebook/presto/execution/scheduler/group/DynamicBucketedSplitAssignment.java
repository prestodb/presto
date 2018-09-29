package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToIntFunction;

public class DynamicBucketedSplitAssignment
        extends BucketedSplitAssignment
{
    private final Map<Integer, Node> bucketToNode = new HashMap<>();

    public DynamicBucketedSplitAssignment(ToIntFunction<Split> splitToBucket)
    {
        super(splitToBucket);
    }

    @Override
    public Optional<Node> getAssignedNode(int bucketedId)
    {
        return Optional.ofNullable(bucketToNode.get(bucketedId));
    }

    @Override
    public void assignOrChangeBucketToNode(int bucketedId, Node node)
    {
        if (!bucketToNode.containsKey(bucketedId)) {
            bucketToNode.put(bucketedId, node);
        }
        else {
            // TODO: Support it for partial recovery
            throw new UnsupportedOperationException();
        }
    }
}