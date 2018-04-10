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
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.drift.client.DriftClient;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ThriftIndexProvider
        implements ConnectorIndexProvider
{
    private final DriftClient<PrestoThriftService> client;
    private final long maxBytesPerResponse;
    private final int lookupRequestsConcurrency;
    private final ThriftConnectorStats stats;
    private final ThriftSessionProperties sessionProperties;

    @Inject
    public ThriftIndexProvider(DriftClient<PrestoThriftService> client, ThriftConnectorStats stats, ThriftConnectorConfig config, ThriftSessionProperties sessionProperties)
    {
        this.client = requireNonNull(client, "client is null");
        this.stats = requireNonNull(stats, "stats is null");
        requireNonNull(config, "config is null");
        this.maxBytesPerResponse = config.getMaxResponseSize().toBytes();
        this.lookupRequestsConcurrency = config.getLookupRequestsConcurrency();
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public ConnectorIndex getIndex(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorIndexHandle indexHandle,
            List<ColumnHandle> lookupSchema,
            List<ColumnHandle> outputSchema)
    {
        return new ThriftConnectorIndex(client, sessionProperties.createHeaderFromSession(session), stats, (ThriftIndexHandle) indexHandle, lookupSchema, outputSchema, maxBytesPerResponse, lookupRequestsConcurrency);
    }
}
