/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.metadata;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import javax.inject.Inject;
import java.net.URI;
import java.util.Set;

public class InMemoryNodeManager
        implements NodeManager {

    private final Node localNode;
    private final SetMultimap<String, Node> remoteNodes = Multimaps.synchronizedSetMultimap(HashMultimap.<String, Node>create());

    @Inject
    public InMemoryNodeManager()
    {
        localNode = new Node("local", URI.create("local"));
    }

    public InMemoryNodeManager(URI localUri)
    {
        localNode = new Node("local", localUri);
    }

    public void addNode(String datasourceName, Node... nodes)
    {
        addNode(datasourceName, ImmutableList.copyOf(nodes));
    }

    public void addNode(String datasourceName, Iterable<? extends Node> nodes)
    {
        remoteNodes.putAll(datasourceName, nodes);
    }

    @Override
    public Set<Node> getActiveDatasourceNodes(String datasourceName)
    {
        return ImmutableSet.copyOf(remoteNodes.get(datasourceName));
    }

    @Override
    public Set<Node> getActiveNodes()
    {
        return ImmutableSet.<Node>builder().add(localNode).addAll(remoteNodes.values()).build();
    }

    @Override
    public Optional<Node> getCurrentNode()
    {
        return Optional.of(localNode);
    }

    @Override
    public void refreshNodes(boolean force)
    {
    }
}
