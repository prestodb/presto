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

package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

public class MemoryConnectorFactory
        implements ConnectorFactory
{
    private final int defaultSplitsPerNode;
    private final NodeManager nodeManager;

    public MemoryConnectorFactory(NodeManager nodeManager)
    {
        this(nodeManager, Runtime.getRuntime().availableProcessors());
    }

    public MemoryConnectorFactory(NodeManager nodeManager, int defaultSplitsPerNode)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.defaultSplitsPerNode = defaultSplitsPerNode;
    }

    @Override
    public String getName()
    {
        return "memory";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new MemoryHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig, ConnectorContext context)
    {
        int splitsPerNode = getSplitsPerNode(requiredConfig);
        MemoryPagesStore pagesStore = new MemoryPagesStore();

        return new MemoryConnector(
                new MemoryMetadata(connectorId, pagesStore),
                new MemorySplitManager(connectorId, nodeManager, splitsPerNode),
                new MemoryPageSourceProvider(pagesStore),
                new MemoryPageSinkProvider(pagesStore));
    }

    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get("in-memory.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property in-memory.splits-per-node");
        }
    }
}
