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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeState;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import javax.inject.Inject;

import java.net.URI;
import java.util.Set;

public class InMemoryNodeManager
        implements InternalNodeManager
{
    private final Node localNode;
    private final SetMultimap<ConnectorId, Node> remoteNodes = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    @Inject
    public InMemoryNodeManager()
    {
        this(URI.create("local://127.0.0.1"));
    }

    public InMemoryNodeManager(URI localUri)
    {
        localNode = new PrestoNode("local", localUri, NodeVersion.UNKNOWN, false);
    }

    public void addCurrentNodeConnector(ConnectorId connectorId)
    {
        addNode(connectorId, localNode);
    }

    public void addNode(ConnectorId connectorId, Node... nodes)
    {
        addNode(connectorId, ImmutableList.copyOf(nodes));
    }

    public void addNode(ConnectorId connectorId, Iterable<Node> nodes)
    {
        remoteNodes.putAll(connectorId, nodes);
    }

    @Override
    public Set<Node> getNodes(NodeState state)
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
    public Set<Node> getActiveConnectorNodes(ConnectorId connectorId)
    {
        return ImmutableSet.copyOf(remoteNodes.get(connectorId));
    }

    @Override
    public AllNodes getAllNodes()
    {
        return new AllNodes(ImmutableSet.<Node>builder().add(localNode).addAll(remoteNodes.values()).build(), ImmutableSet.<Node>of(), ImmutableSet.<Node>of());
    }

    @Override
    public Node getCurrentNode()
    {
        return localNode;
    }

    @Override
    public Set<Node> getCoordinators()
    {
        // always use localNode as coordinator
        return ImmutableSet.of(localNode);
    }

    @Override
    public void refreshNodes()
    {
        // no-op
    }
}
