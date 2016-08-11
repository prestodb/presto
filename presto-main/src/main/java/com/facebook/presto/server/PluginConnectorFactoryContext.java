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
package com.facebook.presto.server;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.ServerInfo;
import com.facebook.presto.spi.connector.ConnectorFactoryContext;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

class PluginConnectorFactoryContext
        implements ConnectorFactoryContext
{
    private final TypeManager typeManager;
    private final NodeManager nodeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final ServerInfo serverInfo;

    @Inject
    public PluginConnectorFactoryContext(
            NodeInfo nodeInfo,
            NodeVersion nodeVersion,
            TypeManager typeManager,
            NodeManager nodeManager,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(nodeVersion, "nodeVersion is null");
        this.serverInfo = new ServerInfo(nodeInfo.getNodeId(), nodeInfo.getEnvironment(), nodeVersion.getVersion());

        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
    }

    @Override
    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public PageSorter getPageSorter()
    {
        return pageSorter;
    }

    @Override
    public PageIndexerFactory getPageIndexerFactory()
    {
        return pageIndexerFactory;
    }

    @Override
    public ServerInfo getServerInfo()
    {
        return serverInfo;
    }
}
