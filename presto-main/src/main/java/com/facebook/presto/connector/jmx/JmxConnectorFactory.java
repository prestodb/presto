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

import com.facebook.presto.connector.InternalConnector;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputHandleResolver;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;
import javax.management.MBeanServer;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class JmxConnectorFactory
        implements ConnectorFactory
{
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
    public Connector create(final String connectorId, Map<String, String> properties)
    {
        return new InternalConnector() {
            @Override
            public ConnectorHandleResolver getHandleResolver()
            {
                return new JmxHandleResolver();
            }

            @Override
            public ConnectorMetadata getMetadata()
            {
                return new JmxMetadata(new JmxConnectorId(connectorId), mbeanServer);
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new JmxSplitManager(new JmxConnectorId(connectorId), nodeManager);
            }

            @Override
            public ConnectorDataStreamProvider getDataStreamProvider()
            {
                return new JmxDataStreamProvider(new JmxConnectorId(connectorId), mbeanServer, nodeInfo);
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorRecordSinkProvider getRecordSinkProvider()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorIndexResolver getIndexResolver()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorOutputHandleResolver getOutputHandleResolver()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
