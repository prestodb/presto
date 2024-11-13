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
package com.facebook.presto.tpcds;

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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNullElse;

public class TpcdsConnectorFactory
        implements ConnectorFactory
{
    private final int defaultSplitsPerNode;

    public TpcdsConnectorFactory()
    {
        this(Runtime.getRuntime().availableProcessors());
    }

    public TpcdsConnectorFactory(int defaultSplitsPerNode)
    {
        checkState(defaultSplitsPerNode > 0, "default splits per node is negative");
        this.defaultSplitsPerNode = defaultSplitsPerNode;
    }

    @Override
    public String getName()
    {
        return "tpcds";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new TpcdsHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        int splitsPerNode = getSplitsPerNode(config);
        NodeManager nodeManager = context.getNodeManager();
        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return TpcdsTransactionHandle.INSTANCE;
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
            {
                return new TpcdsMetadata();
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new TpcdsSplitManager(nodeManager, splitsPerNode, isWithNoSexism(config));
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new TpcdsRecordSetProvider();
            }

            @Override
            public ConnectorNodePartitioningProvider getNodePartitioningProvider()
            {
                return new TpcdsNodePartitioningProvider(nodeManager, splitsPerNode);
            }
        };
    }

    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return parseInt(requireNonNullElse(properties.get("tpcds.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property tpcds.splits-per-node");
        }
    }

    private boolean isWithNoSexism(Map<String, String> properties)
    {
        return parseBoolean(requireNonNullElse(properties.get("tpcds.with-no-sexism"), String.valueOf(false)));
    }
}
