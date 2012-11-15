package com.facebook.presto.split;

import com.facebook.presto.metadata.Node;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class SplitAssignments
{
    private final Split split;
    private final List<Node> nodes;

    public SplitAssignments(Split split, List<Node> nodes)
    {
        this.split = checkNotNull(split, "split is null");
        this.nodes = ImmutableList.copyOf(checkNotNull(nodes, "nodes is null"));
        checkArgument(!nodes.isEmpty(), "nodes is empty");
    }

    public Split getSplit()
    {
        return split;
    }

    public List<Node> getNodes()
    {
        return nodes;
    }
}
