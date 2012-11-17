package com.facebook.presto.split;

import com.facebook.presto.metadata.Node;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SplitNodes
{
    private final Split split;
    private final List<Node> nodes;

    public SplitNodes(Split split, List<Node> nodes)
    {
        this.split = checkNotNull(split, "split is null");
        this.nodes = checkNotNull(nodes, "nodes is null");
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
