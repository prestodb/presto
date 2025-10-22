/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeLoadMetrics;
import com.facebook.presto.spi.NodeState;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import jakarta.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;

public class InMemoryNodeManager
        implements InternalNodeManager
{
    private final InternalNode localNode;
    private final ImmutableSet.Builder<InternalNode> shuttingDownNodesBuilder;
    private final SetMultimap<ConnectorId, InternalNode> remoteNodes = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    @GuardedBy("this")
    private final List<Consumer<AllNodes>> listeners = new ArrayList<>();

    @Inject
    public InMemoryNodeManager()
    {
        this(URI.create("local://127.0.0.1"));
    }

    public InMemoryNodeManager(URI localUri)
    {
        localNode = new InternalNode("local", localUri, NodeVersion.UNKNOWN, false);
        shuttingDownNodesBuilder = ImmutableSet.builder();
    }

    public void addCurrentNodeConnector(ConnectorId connectorId)
    {
        addNode(connectorId, localNode);
    }

    public void addNode(ConnectorId connectorId, InternalNode... nodes)
    {
        addNode(connectorId, ImmutableList.copyOf(nodes));
    }

    public void addShuttingDownNode(InternalNode node)
    {
        shuttingDownNodesBuilder.add(node);
    }

    public void addNode(ConnectorId connectorId, Iterable<InternalNode> nodes)
    {
        remoteNodes.putAll(connectorId, nodes);

        List<Consumer<AllNodes>> listeners;
        synchronized (this) {
            listeners = ImmutableList.copyOf(this.listeners);
        }
        AllNodes allNodes = getAllNodes();
        listeners.forEach(listener -> listener.accept(allNodes));
    }

    @Override
    public Set<InternalNode> getNodes(NodeState state)
    {
        switch (state) {
            case ACTIVE:
                return getAllNodes().getActiveNodes();
            case INACTIVE:
                return getAllNodes().getInactiveNodes();
            case SHUTTING_DOWN:
                return getAllNodes().getShuttingDownNodes();
            default:
                throw new IllegalArgumentException("Unknown node state " + state);
        }
    }

    @Override
    public Set<InternalNode> getActiveConnectorNodes(ConnectorId connectorId)
    {
        return ImmutableSet.copyOf(remoteNodes.get(connectorId));
    }

    @Override
    public Set<InternalNode> getAllConnectorNodes(ConnectorId connectorId)
    {
        return getActiveConnectorNodes(connectorId);
    }

    @Override
    public AllNodes getAllNodes()
    {
        return new AllNodes(
                ImmutableSet.<InternalNode>builder().add(localNode).addAll(remoteNodes.values()).build(),
                ImmutableSet.of(),
                shuttingDownNodesBuilder.build(),
                concat(Stream.of(localNode), remoteNodes.values().stream()).collect(toImmutableSet()).stream().filter(InternalNode::isCoordinator).collect(toImmutableSet()),
                concat(Stream.of(localNode), remoteNodes.values().stream()).collect(toImmutableSet()).stream().filter(InternalNode::isResourceManager).collect(toImmutableSet()),
                concat(Stream.of(localNode), remoteNodes.values().stream()).collect(toImmutableSet()).stream().filter(InternalNode::isCatalogServer).collect(toImmutableSet()),
                concat(Stream.of(localNode), remoteNodes.values().stream()).collect(toImmutableSet()).stream().filter(InternalNode::isCoordinatorSidecar).collect(toImmutableSet()));
    }

    @Override
    public InternalNode getCurrentNode()
    {
        return localNode;
    }

    @Override
    public Set<InternalNode> getCoordinators()
    {
        // always use localNode as coordinator
        return getAllNodes().getActiveCoordinators();
    }

    @Override
    public Set<InternalNode> getShuttingDownCoordinator()
    {
        return getNodes(SHUTTING_DOWN).stream().filter(InternalNode::isCoordinator).collect(toImmutableSet());
    }

    @Override
    public Set<InternalNode> getResourceManagers()
    {
        return getAllNodes().getActiveResourceManagers();
    }

    @Override
    public Set<InternalNode> getCatalogServers()
    {
        return getAllNodes().getActiveCatalogServers();
    }

    @Override
    public Set<InternalNode> getCoordinatorSidecars()
    {
        return getAllNodes().getActiveCoordinatorSidecars();
    }

    @Override
    public void refreshNodes()
    {
        // no-op
    }

    @Override
    public synchronized void addNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.add(requireNonNull(listener, "listener is null"));
    }

    @Override
    public synchronized void removeNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.remove(requireNonNull(listener, "listener is null"));
    }

    @Override
    public Optional<NodeLoadMetrics> getNodeLoadMetrics(String nodeIdentifier)
    {
        return Optional.empty();
    }
}
