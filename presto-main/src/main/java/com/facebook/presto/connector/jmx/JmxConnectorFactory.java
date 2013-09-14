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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.connector.StaticConnector;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.common.collect.ImmutableClassToInstanceMap;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;
import javax.management.MBeanServer;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class JmxConnectorFactory
        implements ConnectorFactory
{
    private static final JmxHandleResolver HANDLE_RESOLVER = new JmxHandleResolver();

    private final MBeanServer mbeanServer;
    private final NodeManager nodeManager;
    private final NodeInfo nodeInfo;

    @Inject
    public JmxConnectorFactory(MBeanServer mbeanServer, NodeManager nodeManager, NodeInfo nodeInfo)
    {
        this.mbeanServer = checkNotNull(mbeanServer, "mbeanServer is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo is null");
    }

    @Override
    public String getName()
    {
        return "jmx";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties)
    {
        JmxConnectorId jmxConnectorId = new JmxConnectorId(connectorId);
        ImmutableClassToInstanceMap.Builder<Object> builder = ImmutableClassToInstanceMap.builder();
        builder.put(ConnectorMetadata.class, new JmxMetadata(jmxConnectorId, mbeanServer));
        builder.put(ConnectorSplitManager.class, new JmxSplitManager(jmxConnectorId, nodeManager));
        builder.put(ConnectorDataStreamProvider.class, new JmxDataStreamProvider(jmxConnectorId, mbeanServer, nodeInfo));
        builder.put(ConnectorHandleResolver.class, HANDLE_RESOLVER);

        return new StaticConnector(builder.build());
    }
}
