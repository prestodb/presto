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
import com.facebook.presto.elasticsearch.ElasticsearchTable;
import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchOutputTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ElasticsearchPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final BaseClient client;

    @Inject
    public ElasticsearchPageSinkProvider(
            BaseClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    /**
     * create table ** as
     */
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        ElasticsearchOutputTableHandle tableHandle = (ElasticsearchOutputTableHandle) outputTableHandle;
        return new ElasticsearchPageSink(client, tableHandle.getSchemaTableName(), tableHandle.getColumns());
    }

    /**
     * insert into ...
     */
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        ElasticsearchTableHandle tableHandle = (ElasticsearchTableHandle) insertTableHandle;
        ElasticsearchTable table = client.getTable(tableHandle.getSchemaTableName());
        List<ElasticsearchColumnHandle> columns = table.getColumns().stream()
                .filter(x -> !x.isHidden()).collect(Collectors.toList());
        return new ElasticsearchPageSink(client, tableHandle.getSchemaTableName(), columns);
    }
}
