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
package com.facebook.presto.testing;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.util.Objects.requireNonNull;

public class TestingNodeManager
        implements NodeManager
{
    private final Node localNode;
    private final Set<Node> nodes = new CopyOnWriteArraySet<>();

    public TestingNodeManager()
    {
        this(new PrestoNode("local", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, false));
    }

    public TestingNodeManager(Node localNode)
    {
        this(localNode, ImmutableSet.of());
    }

    public TestingNodeManager(Node localNode, Collection<Node> otherNodes)
    {
        this.localNode = requireNonNull(localNode, "localNode is null");
        nodes.add(localNode);
        nodes.addAll(otherNodes);
    }

    public void addNode(Node node)
    {
        nodes.add(node);
    }

    @Override
    public Set<Node> getAllNodes()
    {
        return nodes;
    }

    @Override
    public Set<Node> getWorkerNodes()
    {
        return nodes;
    }

    @Override
    public Node getCurrentNode()
    {
        return localNode;
    }

    @Override
    public String getEnvironment()
    {
        return "testenv";
    }
}
