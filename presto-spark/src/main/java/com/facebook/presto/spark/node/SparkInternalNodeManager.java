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

public class SparkInternalNodeManager
        implements InternalNodeManager
{
    @Override
    public Set<InternalNode> getNodes(NodeState state)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<InternalNode> getActiveConnectorNodes(ConnectorId connectorId)
    {
        // TODO: Just to make the Tpch connector happy, refactor
        return ImmutableSet.of(
                new InternalNode("spark1", URI.create("http://127.0.0.1:1111"), NodeVersion.UNKNOWN, false),
                new InternalNode("spark2", URI.create("http://127.0.0.2:1111"), NodeVersion.UNKNOWN, false),
                new InternalNode("spark3", URI.create("http://127.0.0.3:1111"), NodeVersion.UNKNOWN, false),
                new InternalNode("spark4", URI.create("http://127.0.0.4:1111"), NodeVersion.UNKNOWN, false));
    }

    @Override
    public InternalNode getCurrentNode()
    {
        // TODO: Just to make the ConnectorManager#addCatalogConnector happy, refactor
        return new InternalNode("spark1", URI.create("http://127.0.0.1:1111"), NodeVersion.UNKNOWN, false);
    }

    @Override
    public Set<InternalNode> getCoordinators()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AllNodes getAllNodes()
    {
        throw new UnsupportedOperationException();
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
