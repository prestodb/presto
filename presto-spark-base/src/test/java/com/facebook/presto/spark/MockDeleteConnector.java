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
package com.facebook.presto.spark;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingPageSinkProvider;
import com.facebook.presto.testing.TestingSplitManager;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_PAGE_SINK_COMMIT;
import static java.util.Objects.requireNonNull;

/**
 * A mock connector that supports DELETE operations, for use in tests.
 * The default TestingMetadata / Hive connectors do not support beginDelete().
 */
public class MockDeleteConnector
{
    private static final String CONNECTOR_NAME = "delete_test";

    private final DeleteMetadata metadata;

    public MockDeleteConnector()
    {
        this.metadata = new DeleteMetadata();
    }

    public DeleteMetadata getMetadata()
    {
        return metadata;
    }

    public Plugin createPlugin()
    {
        return new DeletePlugin(metadata);
    }

    public static String getConnectorName()
    {
        return CONNECTOR_NAME;
    }

    public static class DeleteTableHandle
            implements ConnectorDeleteTableHandle
    {
        @JsonCreator
        public DeleteTableHandle() {}
    }

    public static class DeleteMetadata
            extends TestingMetadata
    {
        @Override
        public Optional<ColumnHandle> getDeleteRowIdColumn(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return Optional.empty();
        }

        @Override
        public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle)
        {
            return false;
        }

        @Override
        public ConnectorDeleteTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return new DeleteTableHandle();
        }
    }

    public static class DeleteHandleResolver
            extends TestingHandleResolver
    {
        @Override
        public Class<? extends ConnectorDeleteTableHandle> getDeleteTableHandleClass()
        {
            return DeleteTableHandle.class;
        }
    }

    private static class DeleteConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;

        private DeleteConnector(ConnectorMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return TestingTransactionHandle.create();
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new TestingSplitManager(ImmutableList.of());
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return new ConnectorPageSourceProvider()
            {
                @Override
                public ConnectorPageSource createPageSource(
                        ConnectorTransactionHandle transactionHandle,
                        ConnectorSession session,
                        ConnectorSplit split,
                        ConnectorTableLayoutHandle layout,
                        List<ColumnHandle> columns,
                        SplitContext splitContext,
                        RuntimeStats runtimeStats)
                {
                    return new FixedPageSource(ImmutableList.of());
                }
            };
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider()
        {
            return new TestingPageSinkProvider();
        }

        @Override
        public Set<ConnectorCapabilities> getCapabilities()
        {
            return ImmutableSet.of(SUPPORTS_PAGE_SINK_COMMIT);
        }
    }

    private static class DeletePlugin
            implements Plugin
    {
        private final DeleteMetadata metadata;

        private DeletePlugin(DeleteMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new ConnectorFactory()
            {
                @Override
                public String getName()
                {
                    return CONNECTOR_NAME;
                }

                @Override
                public ConnectorHandleResolver getHandleResolver()
                {
                    return new DeleteHandleResolver();
                }

                @Override
                public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                {
                    return new DeleteConnector(metadata);
                }
            });
        }
    }
}
