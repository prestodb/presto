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

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.management.MBeanServer;

import java.util.Set;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;

public class JmxConnector
        implements Connector
{
    private static final Logger log = Logger.get(JmxConnector.class);

    private final JmxMetadata jmxMetadata;
    private final JmxHistoryDumper jmxHistoryDumper;
    private final JmxSplitManager jmxSplitManager;
    private final JmxRecordSetProvider jmxRecordSetProvider;

    public JmxConnector(
            String connectorId,
            MBeanServer mbeanServer,
            NodeManager nodeManager,
            Set<String> dumpTables,
            Duration dumpPeriod,
            int evictionLimit)
    {
        JmxHistoryHolder jmxHistoryHolder = new JmxHistoryHolder(evictionLimit, dumpTables);
        jmxSplitManager = new JmxSplitManager(connectorId, nodeManager);
        jmxMetadata = new JmxMetadata(connectorId, mbeanServer, jmxHistoryHolder);
        jmxRecordSetProvider = new JmxRecordSetProvider(mbeanServer, nodeManager.getCurrentNode().getNodeIdentifier(), jmxHistoryHolder);
        jmxHistoryDumper = new JmxHistoryDumper(jmxHistoryHolder, jmxMetadata, jmxRecordSetProvider, dumpPeriod);
        jmxHistoryDumper.start();
    }

    @Override
    public JmxMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return jmxMetadata;
    }

    @Override
    public JmxSplitManager getSplitManager()
    {
        return jmxSplitManager;
    }

    @Override
    public JmxRecordSetProvider getRecordSetProvider()
    {
        return jmxRecordSetProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return JmxTransactionHandle.INSTANCE;
    }

    @Override
    public void shutdown()
    {
        try {
            jmxHistoryDumper.shutdown();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
