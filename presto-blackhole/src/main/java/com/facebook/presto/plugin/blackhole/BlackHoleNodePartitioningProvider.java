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
package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

import static com.facebook.presto.plugin.blackhole.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Objects.requireNonNull;

public class BlackHoleNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final String connectorId;
    private final NodeManager nodeManager;

    public BlackHoleNodePartitioningProvider(String connectorId, NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public Map<Integer, Node> getBucketToNode(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        Set<Node> nodes = nodeManager.getActiveDatasourceNodes(connectorId);
        if (nodes.isEmpty()) {
            throw new PrestoException(NO_NODES_AVAILABLE, "No black hole nodes available");
        }

        BlackHolePartitioningHandle handle = checkType(partitioningHandle, BlackHolePartitioningHandle.class, "partitionHandle");

        // create on part per node
        ImmutableMap.Builder<Integer, Node> bucketToNode = ImmutableMap.builder();
        Iterator<Node> iterator = Iterables.cycle(nodes).iterator();
        for (int bucketNumber = 0; bucketNumber < handle.getBucketCount(); bucketNumber++) {
            bucketToNode.put(bucketNumber, iterator.next());
        }
        return bucketToNode.build();
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> {
            throw new PrestoException(NOT_SUPPORTED, "Black hole connector does not supported distributed reads");
        };
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        BlackHolePartitioningHandle handle = checkType(partitioningHandle, BlackHolePartitioningHandle.class, "partitionHandle");
        int bucketCount = handle.getBucketCount();
        List<Type> partitionChannelTypes = handle.getPartitionChannelTypes();
        return (page, position) -> {
            long hash = 13;
            for (int i = 0; i < partitionChannelTypes.size(); i++) {
                Type type = partitionChannelTypes.get(i);
                hash = 31 * hash + type.hash(page.getBlock(i), position);
            }

            // clear the sign bit
            hash &= 0x7fff_ffff_ffff_ffffL;

            return (int) (hash % bucketCount);
        };
    }
}
