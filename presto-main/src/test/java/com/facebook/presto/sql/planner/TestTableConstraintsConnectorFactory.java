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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchNodePartitioningProvider;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchSplitManager;
import com.facebook.presto.tpch.TpchTransactionHandle;

import java.util.Map;

public class TestTableConstraintsConnectorFactory
        extends TpchConnectorFactory
{
    public TestTableConstraintsConnectorFactory(int defaultSplitsPerNode)
    {
        super(defaultSplitsPerNode);
    }

    @Override
    public Connector create(String catalogName, Map<String, String> properties, ConnectorContext context)
    {
        int splitsPerNode = super.getSplitsPerNode(properties);
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
                return new TestTableConstraintsMetadata(catalogName, columnNaming, isPredicatePushdownEnabled(), isPartitioningEnabled(properties));
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
}
