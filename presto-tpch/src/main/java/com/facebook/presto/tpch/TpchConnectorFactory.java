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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.NodeManager;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchConnectorFactory
        implements ConnectorFactory
{
    private final NodeManager nodeManager;
    private final int defaultSplitsPerNode;

    public TpchConnectorFactory(NodeManager nodeManager)
    {
        this(nodeManager, Runtime.getRuntime().availableProcessors());
    }

    public TpchConnectorFactory(NodeManager nodeManager, int defaultSplitsPerNode)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.defaultSplitsPerNode = defaultSplitsPerNode;
    }

    @Override
    public String getName()
    {
        return "tpch";
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> properties)
    {
        final int splitsPerNode = getSplitsPerNode(properties);

        return new Connector() {
            @Override
            public ConnectorMetadata getMetadata()
            {
                return new TpchMetadata(connectorId);
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new TpchSplitManager(connectorId, nodeManager, splitsPerNode);
            }

            @Override
            public ConnectorHandleResolver getHandleResolver()
            {
                return new TpchHandleResolver(connectorId);
            }

            @Override
            public ConnectorPageSourceProvider getPageSourceProvider()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new TpchRecordSetProvider();
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
        };
    }

    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get("tpch.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property tpch.splits-per-node");
        }
    }
}
