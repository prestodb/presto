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
package com.facebook.presto.spark.node;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeState;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.collect.Sets.union;

public class PrestoSparkInternalNodeManager
        implements InternalNodeManager
{
    private static final InternalNode CURRENT_NODE = new InternalNode("presto-spark-current", URI.create("http://127.0.0.1:60040"), NodeVersion.UNKNOWN, true);
    private static final Set<InternalNode> OTHER_NODES = ImmutableSet.of(
            new InternalNode("presto-spark-other-1", URI.create("http://127.0.0.1:60041"), NodeVersion.UNKNOWN, false),
            new InternalNode("presto-spark-other-2", URI.create("http://127.0.0.1:60042"), NodeVersion.UNKNOWN, false),
            new InternalNode("presto-spark-other-3", URI.create("http://127.0.0.1:60043"), NodeVersion.UNKNOWN, false));
    private static final AllNodes ALL_NODES = new AllNodes(
            union(ImmutableSet.of(CURRENT_NODE), OTHER_NODES),
            ImmutableSet.of(),
            ImmutableSet.of(),
            ImmutableSet.of(),
            ImmutableSet.of());

    @Override
    public Set<InternalNode> getNodes(NodeState state)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<InternalNode> getActiveConnectorNodes(ConnectorId connectorId)
    {
        // TODO: Hack to make TPCH connector work
        return ALL_NODES.getActiveNodes();
    }

    @Override
    public Set<InternalNode> getAllConnectorNodes(ConnectorId connectorId)
    {
        return getActiveConnectorNodes(connectorId);
    }

    @Override
    public InternalNode getCurrentNode()
    {
        // TODO: Hack to make TPCH connector work
        return CURRENT_NODE;
    }

    @Override
    public Set<InternalNode> getCoordinators()
    {
        // TODO: Hack to make System connector work
        return ImmutableSet.of(CURRENT_NODE);
    }

    @Override
    public Set<InternalNode> getShuttingDownCoordinator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<InternalNode> getResourceManagers()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AllNodes getAllNodes()
    {
        // TODO: Hack to make System connector work
        return ALL_NODES;
    }

    @Override
    public void refreshNodes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addNodeChangeListener(Consumer<AllNodes> listener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeNodeChangeListener(Consumer<AllNodes> listener)
    {
        throw new UnsupportedOperationException();
    }
}
