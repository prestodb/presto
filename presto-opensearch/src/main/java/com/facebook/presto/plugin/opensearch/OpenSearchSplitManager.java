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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Split manager for OpenSearch connector.
 * Creates splits based on OpenSearch shards for parallel execution.
 */
public class OpenSearchSplitManager
        implements ConnectorSplitManager
{
    private final OpenSearchClient client;

    @Inject
    public OpenSearchSplitManager(OpenSearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        OpenSearchTableLayoutHandle layoutHandle = (OpenSearchTableLayoutHandle) layout;
        OpenSearchTableHandle table = layoutHandle.getTable();

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        // For k-NN searches, create a single split that queries all shards
        // k-NN search must be executed globally, not per shard
        if (table.isKnnSearch()) {
            // Get shard information to determine node address
            List<OpenSearchClient.ShardInfo> shards = client.getShardInfo(table.getIndexName());

            // Use the first shard's node address (k-NN search will query all shards)
            List<HostAddress> addresses = shards.isEmpty()
                    ? ImmutableList.of()
                    : ImmutableList.of(HostAddress.fromString(shards.get(0).getNodeAddress()));

            OpenSearchSplit split = new OpenSearchSplit(
                    table.getIndexName(),
                    0, // Use shard 0 as placeholder since we query all shards
                    addresses,
                    table.getVectorField().get(),
                    table.getQueryVector().get(),
                    table.getK().get(),
                    table.getSpaceType().get(),
                    table.getEfSearch().orElse(null));

            splits.add(split);
        }
        else {
            // For regular queries, get shard information from OpenSearch
            List<OpenSearchClient.ShardInfo> shards = client.getShardInfo(table.getIndexName());

            // Create one split per shard for parallel execution
            for (OpenSearchClient.ShardInfo shard : shards) {
                List<HostAddress> addresses = ImmutableList.of(
                        HostAddress.fromString(shard.getNodeAddress()));

                OpenSearchSplit split = new OpenSearchSplit(
                        table.getIndexName(),
                        shard.getShardId(),
                        addresses);

                splits.add(split);
            }
        }

        return new FixedSplitSource(splits.build());
    }
}
