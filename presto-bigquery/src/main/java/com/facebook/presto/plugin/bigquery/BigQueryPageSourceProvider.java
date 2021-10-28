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
package com.facebook.presto.plugin.bigquery;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class BigQueryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(BigQueryPageSourceProvider.class);
    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;
    private final int maxReadRowsRetries;

    @Inject
    public BigQueryPageSourceProvider(BigQueryStorageClientFactory bigQueryStorageClientFactory, BigQueryConfig config)
    {
        this.bigQueryStorageClientFactory = bigQueryStorageClientFactory;
        this.maxReadRowsRetries = config.getMaxReadRowsRetries();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        BigQuerySplit bigQuerySplit = (BigQuerySplit) split;
        if (bigQuerySplit.representsEmptyProjection()) {
            return new BigQueryEmptySplitPageSource(bigQuerySplit.getEmptyRowsToGenerate());
        }

        // not empty projection
        BigQueryTableLayoutHandle bigQueryTableLayoutHandle = (BigQueryTableLayoutHandle) layout;
        BigQueryTableHandle bigQueryTableHandle = bigQueryTableLayoutHandle.getTable();
        ImmutableList<BigQueryColumnHandle> bigQueryColumnHandles = columns.stream()
                .map(BigQueryColumnHandle.class::cast)
                .collect(toImmutableList());

        return new BigQueryResultPageSource(bigQueryStorageClientFactory, maxReadRowsRetries, bigQuerySplit, bigQueryTableHandle, bigQueryColumnHandles);
    }
}
