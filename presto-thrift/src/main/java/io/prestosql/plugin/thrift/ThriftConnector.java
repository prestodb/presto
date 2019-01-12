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
package io.prestosql.plugin.thrift;

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorIndexProvider;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ThriftConnector
        implements Connector
{
    private static final Logger log = Logger.get(ThriftConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final ThriftMetadata metadata;
    private final ThriftSplitManager splitManager;
    private final ThriftPageSourceProvider pageSourceProvider;
    private final ThriftSessionProperties sessionProperties;
    private final ThriftIndexProvider indexProvider;

    @Inject
    public ThriftConnector(
            LifeCycleManager lifeCycleManager,
            ThriftMetadata metadata,
            ThriftSplitManager splitManager,
            ThriftPageSourceProvider pageSourceProvider,
            ThriftSessionProperties sessionProperties,
            ThriftIndexProvider indexProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.indexProvider = requireNonNull(indexProvider, "indexProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return ThriftTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public ConnectorIndexProvider getIndexProvider()
    {
        return indexProvider;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error(ie, "Interrupted while shutting down connector");
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
