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
package com.facebook.presto.delta;

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.delta.rule.DeltaPlanOptimizerProvider;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.delta.DeltaTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class DeltaConnector
        implements Connector
{
    private static final Logger log = Logger.get(DeltaConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final DeltaMetadata metadata;
    private final DeltaSplitManager splitManager;
    private final DeltaSessionProperties sessionProperties;
    private final DeltaPageSourceProvider pageSourceProvider;
    private final DeltaPlanOptimizerProvider planOptimizerProvider;

    @Inject
    public DeltaConnector(
            LifeCycleManager lifeCycleManager,
            DeltaMetadata metadata,
            DeltaSplitManager splitManager,
            DeltaSessionProperties sessionProperties,
            DeltaPageSourceProvider pageSourceProvider,
            DeltaPlanOptimizerProvider planOptimizerProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.planOptimizerProvider = requireNonNull(planOptimizerProvider, "planOptimizerProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return new ClassLoaderSafeConnectorMetadata(metadata, getClass().getClassLoader());
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new ClassLoaderSafeConnectorSplitManager(splitManager, getClass().getClassLoader());
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return new ClassLoaderSafeConnectorPageSourceProvider(pageSourceProvider, getClass().getClassLoader());
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider()
    {
        return planOptimizerProvider;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception exception) {
            log.error(exception, "Error shutting down connector");
        }
    }
}
