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
package io.prestosql.plugin.thrift;

import io.airlift.drift.client.DriftClient;
import io.prestosql.plugin.thrift.api.PrestoThriftService;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorIndex;
import io.prestosql.spi.connector.ConnectorIndexHandle;
import io.prestosql.spi.connector.ConnectorIndexProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ThriftIndexProvider
        implements ConnectorIndexProvider
{
    private final DriftClient<PrestoThriftService> client;
    private final ThriftHeaderProvider thriftHeaderProvider;
    private final long maxBytesPerResponse;
    private final int lookupRequestsConcurrency;
    private final ThriftConnectorStats stats;

    @Inject
    public ThriftIndexProvider(DriftClient<PrestoThriftService> client, ThriftHeaderProvider thriftHeaderProvider, ThriftConnectorStats stats, ThriftConnectorConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.thriftHeaderProvider = requireNonNull(thriftHeaderProvider, "thriftHeaderProvider is null");
        this.stats = requireNonNull(stats, "stats is null");
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
        return new ThriftConnectorIndex(
                client,
                thriftHeaderProvider.getHeaders(session),
                stats,
                (ThriftIndexHandle) indexHandle,
                lookupSchema,
                outputSchema,
                maxBytesPerResponse,
                lookupRequestsConcurrency);
    }
}
