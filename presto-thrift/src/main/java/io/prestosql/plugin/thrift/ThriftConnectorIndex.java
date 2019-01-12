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
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.RecordSet;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ThriftConnectorIndex
        implements ConnectorIndex
{
    private final DriftClient<PrestoThriftService> client;
    private final Map<String, String> thriftHeaders;
    private final ThriftIndexHandle indexHandle;
    private final List<ColumnHandle> lookupColumns;
    private final List<ColumnHandle> outputColumns;
    private final long maxBytesPerResponse;
    private final int lookupRequestsConcurrency;
    private final ThriftConnectorStats stats;

    public ThriftConnectorIndex(
            DriftClient<PrestoThriftService> client,
            Map<String, String> thriftHeaders,
            ThriftConnectorStats stats,
            ThriftIndexHandle indexHandle,
            List<ColumnHandle> lookupColumns,
            List<ColumnHandle> outputColumns,
            long maxBytesPerResponse,
            int lookupRequestsConcurrency)
    {
        this.client = requireNonNull(client, "client is null");
        this.thriftHeaders = requireNonNull(thriftHeaders, "thriftHeaders is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.indexHandle = requireNonNull(indexHandle, "indexHandle is null");
        this.lookupColumns = requireNonNull(lookupColumns, "lookupColumns is null");
        this.outputColumns = requireNonNull(outputColumns, "outputColumns is null");
        this.maxBytesPerResponse = maxBytesPerResponse;
        this.lookupRequestsConcurrency = lookupRequestsConcurrency;
    }

    @Override
    public ConnectorPageSource lookup(RecordSet recordSet)
    {
        return new ThriftIndexPageSource(client, thriftHeaders, stats, indexHandle, lookupColumns, outputColumns, recordSet, maxBytesPerResponse, lookupRequestsConcurrency);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("indexHandle", indexHandle)
                .add("lookupColumns", lookupColumns)
                .add("outputColumns", outputColumns)
                .toString();
    }
}
