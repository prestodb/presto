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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardNode;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.raptor.util.Nodes.nodeIdentifier;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.String.format;

public class RaptorSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final NodeManager nodeManager;
    private final ShardManager shardManager;

    @Inject
    public RaptorSplitManager(RaptorConnectorId connectorId, NodeManager nodeManager, ShardManager shardManager)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        RaptorTableHandle handle = checkType(tableHandle, RaptorTableHandle.class, "table");
        ConnectorPartition partition = new RaptorPartition(handle.getTableId(), tupleDomain);
        return new ConnectorPartitionResult(ImmutableList.of(partition), tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        RaptorTableHandle raptorTableHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");

        checkArgument(partitions.size() == 1, "expected exactly one partition");
        RaptorPartition partition = checkType(getOnlyElement(partitions), RaptorPartition.class, "partition");
        TupleDomain<RaptorColumnHandle> effectivePredicate = toRaptorTupleDomain(partition.getEffectivePredicate());

        Map<String, Node> nodesById = uniqueIndex(nodeManager.getActiveNodes(), nodeIdentifier());

        ImmutableMultimap.Builder<UUID, String> shardNodes = ImmutableMultimap.builder();
        for (ShardNode shardNode : shardManager.getShardNodes(raptorTableHandle.getTableId())) {
            shardNodes.put(shardNode.getShardUuid(), shardNode.getNodeIdentifier());
        }

        List<ConnectorSplit> splits = new ArrayList<>();
        for (Map.Entry<UUID, Collection<String>> entry : shardNodes.build().asMap().entrySet()) {
            UUID shardId = entry.getKey();
            Collection<String> nodeId = entry.getValue();
            List<HostAddress> addresses = getAddressesForNodes(nodesById, nodeId);
            if (addresses.isEmpty()) {
                throw new PrestoException(INTERNAL_ERROR, format("no host for shard %s found: %s", shardId, nodeId));
            }
            splits.add(new RaptorSplit(shardId, addresses, effectivePredicate));
        }

        // The query engine assumes that splits are returned in a somewhat random fashion. The Raptor split manager,
        // because it loads the data from a database table, will return the splits somewhat ordered by node ID,
        // so only a subset of nodes are fired up. Shuffle the splits to ensure random distribution.
        Collections.shuffle(splits);

        return new FixedSplitSource(connectorId, splits);
    }

    private static List<HostAddress> getAddressesForNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        ImmutableList.Builder<HostAddress> nodes = ImmutableList.builder();
        for (String id : nodeIdentifiers) {
            Node node = nodeMap.get(id);
            if (node != null) {
                nodes.add(node.getHostAndPort());
            }
        }
        return nodes.build();
    }

    @SuppressWarnings("unchecked")
    private static TupleDomain<RaptorColumnHandle> toRaptorTupleDomain(TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        return tupleDomain.transform(new TupleDomain.Function<ConnectorColumnHandle, RaptorColumnHandle>()
        {
            @Override
            public RaptorColumnHandle apply(ConnectorColumnHandle handle)
            {
                return checkType(handle, RaptorColumnHandle.class, "columnHandle");
            }
        });
    }
}
