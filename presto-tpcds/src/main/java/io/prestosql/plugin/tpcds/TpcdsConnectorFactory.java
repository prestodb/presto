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
package io.prestosql.plugin.tpcds;

import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

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
            return parseInt(firstNonNull(properties.get("tpcds.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property tpcds.splits-per-node");
        }
    }

    private boolean isWithNoSexism(Map<String, String> properties)
    {
        return parseBoolean(firstNonNull(properties.get("tpcds.with-no-sexism"), String.valueOf(false)));
    }
}
