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
package io.prestosql.plugin.jmx;

import io.airlift.log.Logger;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class JmxConnector
        implements Connector
{
    private static final Logger log = Logger.get(JmxConnector.class);

    private final JmxMetadata jmxMetadata;
    private final JmxPeriodicSampler jmxPeriodicSampler;
    private final JmxSplitManager jmxSplitManager;
    private final JmxRecordSetProvider jmxRecordSetProvider;

    @Inject
    public JmxConnector(
            JmxMetadata jmxMetadata,
            JmxSplitManager jmxSplitManager,
            JmxRecordSetProvider jmxRecordSetProvider,
            JmxPeriodicSampler jmxPeriodicSampler)
    {
        this.jmxMetadata = requireNonNull(jmxMetadata, "jmxMetadata is null");
        this.jmxSplitManager = requireNonNull(jmxSplitManager, "jmxSplitManager is null");
        this.jmxRecordSetProvider = requireNonNull(jmxRecordSetProvider, "jmxRecordSetProvider is null");
        this.jmxPeriodicSampler = requireNonNull(jmxPeriodicSampler, "jmxHistoryDumper is null");
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
            jmxPeriodicSampler.shutdown();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
