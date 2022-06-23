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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;

public class TpchConnectorFactory
        implements ConnectorFactory
{
    public static final String TPCH_COLUMN_NAMING_PROPERTY = "tpch.column-naming";

    private final int defaultSplitsPerNode;
    private final boolean predicatePushdownEnabled;
    private final boolean partitioningEnabled;

    public TpchConnectorFactory()
    {
        this(Runtime.getRuntime().availableProcessors());
    }

    public TpchConnectorFactory(int defaultSplitsPerNode)
    {
        this(defaultSplitsPerNode, true, true);
    }

    public TpchConnectorFactory(int defaultSplitsPerNode, boolean predicatePushdownEnabled, boolean partitioningEnabled)
    {
        this.defaultSplitsPerNode = defaultSplitsPerNode;
        this.predicatePushdownEnabled = predicatePushdownEnabled;
        this.partitioningEnabled = partitioningEnabled;
    }

    @Override
    public String getName()
    {
        return "tpch";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new TpchHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> properties, ConnectorContext context)
    {
        int splitsPerNode = getSplitsPerNode(properties);
        ColumnNaming columnNaming = ColumnNaming.valueOf(properties.getOrDefault(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.SIMPLIFIED.name()).toUpperCase());
        NodeManager nodeManager = context.getNodeManager();

        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return TpchTransactionHandle.INSTANCE;
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return new TpchMetadata(catalogName, columnNaming, predicatePushdownEnabled, isPartitioningEnabled(properties));
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new TpchSplitManager(nodeManager, splitsPerNode);
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new TpchRecordSetProvider();
            }

            @Override
            public ConnectorNodePartitioningProvider getNodePartitioningProvider()
            {
                return new TpchNodePartitioningProvider(nodeManager, splitsPerNode);
            }
        };
    }

    protected int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get("tpch.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property tpch.splits-per-node");
        }
    }

    protected boolean isPartitioningEnabled(Map<String, String> properties)
    {
        return Boolean.parseBoolean(properties.getOrDefault("tpch.partitioning-enabled", String.valueOf(partitioningEnabled)));
    }

    protected boolean isPredicatePushdownEnabled()
    {
        return predicatePushdownEnabled;
    }
}
