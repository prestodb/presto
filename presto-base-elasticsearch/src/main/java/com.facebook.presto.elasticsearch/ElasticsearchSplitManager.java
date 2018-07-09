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

import com.facebook.presto.elasticsearch.model.ElasticsearchSplit;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ElasticsearchSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final BaseClient client;

    @Inject
    public ElasticsearchSplitManager(
            ElasticsearchConnectorId connectorId,
            BaseClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        ElasticsearchTableLayoutHandle layoutHandle = (ElasticsearchTableLayoutHandle) layout;
        ElasticsearchTableHandle tableHandle = layoutHandle.getTable();

//        // Call out to our client to retrieve all tablet split metadata using the row ID domain and the secondary index
        List<ElasticsearchSplit> tabletSplits = client.getTabletSplits(session, tableHandle, layoutHandle, splitSchedulingStrategy); //tableHandle.getSerializerInstance()

        // Pack the tablet split metadata into a connector split
        return new FixedSplitSource(tabletSplits);
    }
}
