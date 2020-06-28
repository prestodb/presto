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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplitManager
        implements ConnectorSplitManager
{
    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchSplitManager(ElasticsearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        ElasticsearchTableLayoutHandle layoutHandle = (ElasticsearchTableLayoutHandle) layout;
        ElasticsearchTableHandle tableHandle = layoutHandle.getTable();
        ElasticsearchTableDescription table = client.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        verify(table != null, "Table no longer exists: %s", tableHandle.toString());

        List<String> indices = client.getIndices(table);
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (String index : indices) {
            ClusterSearchShardsResponse response = client.getSearchShards(index, table);
            DiscoveryNode[] nodes = response.getNodes();
            for (ClusterSearchShardsGroup group : response.getGroups()) {
                int nodeIndex = group.getShardId().getId() % nodes.length;
                ElasticsearchSplit split = new ElasticsearchSplit(
                        index,
                        table.getType(),
                        group.getShardId().getId(),
                        nodes[nodeIndex].getHostName(),
                        nodes[nodeIndex].getAddress().getPort(),
                        layoutHandle.getTupleDomain());
                splits.add(split);
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
