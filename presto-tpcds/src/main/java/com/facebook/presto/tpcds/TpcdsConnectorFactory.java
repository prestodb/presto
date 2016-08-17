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
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

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
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        return new Connector() {
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
                throw new NotImplementedException();
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                throw new NotImplementedException();
            }

            @Override
            public ConnectorNodePartitioningProvider getNodePartitioningProvider()
            {
                throw new NotImplementedException();
            }
        };
    }
}
