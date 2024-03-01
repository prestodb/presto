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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.elasticsearch.client.ElasticsearchClient;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.elasticsearch.ElasticsearchTableHandle.Type.QUERY;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ElasticsearchPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ElasticsearchClient client;
    private final Type jsonType;

    @Inject
    public ElasticsearchPageSourceProvider(ElasticsearchClient client, TypeManager typeManager)
    {
        this.client = requireNonNull(client, "client is null");
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
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
        requireNonNull(split, "split is null");
        requireNonNull(layout, "layout is null");
        ElasticsearchTableLayoutHandle layoutHandle = (ElasticsearchTableLayoutHandle) layout;
        ElasticsearchSplit elasticsearchSplit = (ElasticsearchSplit) split;

        if (layoutHandle.getTable().getType().equals(QUERY)) {
            return new PassthroughQueryPageSource(client, layoutHandle.getTable(), jsonType);
        }

        if (columns.isEmpty()) {
            return new CountQueryPageSource(client, session, layoutHandle.getTable(), elasticsearchSplit);
        }
        return new ScanQueryPageSource(
                client,
                session,
                layoutHandle.getTable(),
                elasticsearchSplit,
                columns.stream()
                        .map(ElasticsearchColumnHandle.class::cast)
                        .collect(toImmutableList()));
    }
}
