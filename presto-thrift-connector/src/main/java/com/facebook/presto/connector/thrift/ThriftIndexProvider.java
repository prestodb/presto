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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ThriftIndexProvider
        implements ConnectorIndexProvider
{
    private final PrestoThriftServiceProvider clientProvider;
    private final long maxBytesPerResponse;
    private final int lookupRequestsConcurrency;

    @Inject
    public ThriftIndexProvider(PrestoThriftServiceProvider clientProvider, ThriftConnectorConfig config)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        requireNonNull(config, "config is null");
        this.maxBytesPerResponse = config.getMaxResponseSize().toBytes();
        this.lookupRequestsConcurrency = config.getLookupRequestsConcurrency();
    }

    @Override
    public ConnectorIndex getIndex(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorIndexHandle indexHandle,
            List<ColumnHandle> lookupSchema,
            List<ColumnHandle> outputSchema)
    {
        return new ThriftConnectorIndex(clientProvider, (ThriftIndexHandle) indexHandle, lookupSchema, outputSchema, maxBytesPerResponse, lookupRequestsConcurrency);
    }
}
