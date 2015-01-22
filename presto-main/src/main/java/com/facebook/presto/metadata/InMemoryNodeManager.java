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

import com.facebook.presto.spi.Node;
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
    private final SetMultimap<String, Node> remoteNodes = Multimaps.synchronizedSetMultimap(HashMultimap.<String, Node>create());

    @Inject
    public InMemoryNodeManager()
    {
        this(URI.create("local://127.0.0.1"));
    }

    public InMemoryNodeManager(URI localUri)
    {
        localNode = new PrestoNode("local", localUri, NodeVersion.UNKNOWN);
    }

    public void addCurrentNodeDatasource(String datasourceName)
    {
        addNode(datasourceName, localNode);
    }

    public void addNode(String datasourceName, Node... nodes)
    {
        addNode(datasourceName, ImmutableList.copyOf(nodes));
    }

    public void addNode(String datasourceName, Iterable<Node> nodes)
    {
        remoteNodes.putAll(datasourceName, nodes);
    }

    @Override
    public Set<Node> getActiveNodes()
    {
        return getAllNodes().getActiveNodes();
    }

    @Override
    public Set<Node> getActiveDatasourceNodes(String datasourceName)
    {
        return ImmutableSet.copyOf(remoteNodes.get(datasourceName));
    }

    @Override
    public AllNodes getAllNodes()
    {
        return new AllNodes(ImmutableSet.<Node>builder().add(localNode).addAll(remoteNodes.values()).build(), ImmutableSet.<Node>of());
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
