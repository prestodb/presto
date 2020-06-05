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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.TransportAddress;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static com.facebook.presto.elasticsearch.ElasticsearchClient.createTransportClient;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CONNECTION_ERROR;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ElasticsearchRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final ElasticsearchConfig config;
    private final LoadingCache<HostAndPort, TransportClient> clients = CacheBuilder.newBuilder()
            .build(CacheLoader.from(this::initializeClient));

    @Inject
    public ElasticsearchRecordSetProvider(ElasticsearchConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        ElasticsearchSplit elasticsearchSplit = (ElasticsearchSplit) split;

        ImmutableList.Builder<ElasticsearchColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((ElasticsearchColumnHandle) handle);
        }

        try {
            TransportClient client = clients.getUnchecked(HostAndPort.fromParts(elasticsearchSplit.getSearchNode(), elasticsearchSplit.getPort()));
            return new ElasticsearchRecordSet(client, elasticsearchSplit, config, handles.build());
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private TransportClient initializeClient(HostAndPort address)
    {
        try {
            return createTransportClient(config, new TransportAddress(InetAddress.getByName(address.getHost()), address.getPort()));
        }
        catch (UnknownHostException e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, format("Failed to resolve search node (%s)", address), e);
        }
    }

    @PreDestroy
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            clients.asMap()
                    .values()
                    .forEach(closer::register);
        }
    }
}
