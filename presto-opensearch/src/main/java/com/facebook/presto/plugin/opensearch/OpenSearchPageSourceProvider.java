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
package com.facebook.presto.plugin.opensearch;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import jakarta.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Page source provider for OpenSearch connector.
 * Creates page sources for reading data from OpenSearch.
 */
public class OpenSearchPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final OpenSearchClient client;
    private final NestedValueExtractor nestedValueExtractor;
    private final QueryBuilder queryBuilder;

    @Inject
    public OpenSearchPageSourceProvider(
            OpenSearchClient client,
            NestedValueExtractor nestedValueExtractor,
            QueryBuilder queryBuilder)
    {
        this.client = requireNonNull(client, "client is null");
        this.nestedValueExtractor = requireNonNull(nestedValueExtractor, "nestedValueExtractor is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext,
            RuntimeStats runtimeStats)
    {
        requireNonNull(split, "split is null");
        requireNonNull(layout, "layout is null");

        OpenSearchSplit openSearchSplit = (OpenSearchSplit) split;

        List<OpenSearchColumnHandle> openSearchColumns = columns.stream()
                .map(OpenSearchColumnHandle.class::cast)
                .collect(toList());

        return new OpenSearchPageSource(client, openSearchSplit, openSearchColumns, nestedValueExtractor, queryBuilder);
    }
}
