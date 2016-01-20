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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.management.MBeanServer;

import java.util.Map;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class JmxConnectorFactory
        implements ConnectorFactory
{
    private final MBeanServer mbeanServer;
    private final NodeManager nodeManager;

    public JmxConnectorFactory(MBeanServer mbeanServer, NodeManager nodeManager)
    {
        this.mbeanServer = requireNonNull(mbeanServer, "mbeanServer is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public String getName()
    {
        return "jmx";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new JmxHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties)
    {
        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                checkConnectorSupports(READ_COMMITTED, isolationLevel);
                return JmxTransactionHandle.INSTANCE;
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
            {
                return new JmxMetadata(connectorId, mbeanServer);
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new JmxSplitManager(connectorId, nodeManager);
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new JmxRecordSetProvider(mbeanServer, nodeManager.getCurrentNode().getNodeIdentifier());
            }
        };
    }
}
