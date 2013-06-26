package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class AllNodes
{
    private final Set<Node> activeNodes;
    private final Set<Node> inactiveNodes;

    public AllNodes(Set<Node> activeNodes, Set<Node> inactiveNodes)
    {
        this.activeNodes = ImmutableSet.copyOf(checkNotNull(activeNodes, "activeNodes is null"));
        this.inactiveNodes = ImmutableSet.copyOf(checkNotNull(inactiveNodes, "inactiveNodes is null"));
    }

    public Set<Node> getActiveNodes()
    {
        return activeNodes;
    }

    public Set<Node> getInactiveNodes()
    {
        return inactiveNodes;
    }
}
