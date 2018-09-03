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
package com.facebook.presto.elasticsearch.io;

import com.facebook.presto.elasticsearch.BaseClient;
import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ElasticsearchPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final BaseClient elasticsearchClient;

    @Inject
    public ElasticsearchPageSourceProvider(BaseClient elasticsearchClient)
    {
        this.elasticsearchClient = requireNonNull(elasticsearchClient, "elasticsearchClient is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        ElasticsearchSplit elasticsearchSplit = (ElasticsearchSplit) split;

        ImmutableList.Builder<ElasticsearchColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : requireNonNull(columns, "columns is null")) {
            handles.add((ElasticsearchColumnHandle) handle);
        }

        return new ElasticsearchPageSource(elasticsearchClient, elasticsearchSplit, handles.build());
    }
}
