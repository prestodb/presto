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

import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.drift.client.DriftClient;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ThriftPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DriftClient<PrestoThriftService> client;
    private final ThriftHeaderProvider thriftHeaderProvider;
    private final long maxBytesPerResponse;
    private final ThriftConnectorStats stats;

    @Inject
    public ThriftPageSourceProvider(DriftClient<PrestoThriftService> client, ThriftHeaderProvider thriftHeaderProvider, ThriftConnectorStats stats, ThriftConnectorConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.thriftHeaderProvider = requireNonNull(thriftHeaderProvider, "thriftHeaderFactor is null");
        this.maxBytesPerResponse = requireNonNull(config, "config is null").getMaxResponseSize().toBytes();
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns)
    {
        return new ThriftPageSource(client, thriftHeaderProvider.getHeaders(session), (ThriftConnectorSplit) split, columns, stats, maxBytesPerResponse);
    }
}
