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
package com.facebook.plugin.arrow;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ArrowConnector
        implements Connector
{
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorHandleResolver handleResolver;

    @Inject
    public ArrowConnector(ConnectorMetadata metadata,
                          ConnectorHandleResolver handleResolver,
                          ConnectorSplitManager splitManager,
                          ConnectorPageSourceProvider pageSourceProvider)
    {
        this.metadata = requireNonNull(metadata, "Metadata is null");
        this.handleResolver = requireNonNull(handleResolver, "Metadata is null");
        this.splitManager = requireNonNull(splitManager, "SplitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "PageSinkProvider is null");
    }

    public Optional<ConnectorHandleResolver> getHandleResolver()
    {
        return Optional.of(handleResolver);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return ArrowTransactionHandle.INSTANCE;
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
}
