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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public class RaptorNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeSupplier nodeSupplier;

    @Inject
    public RaptorNodePartitioningProvider(NodeSupplier nodeSupplier)
    {
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning, List<Node> sortedNodes)
    {
        RaptorPartitioningHandle handle = (RaptorPartitioningHandle) partitioning;

        Map<String, Node> nodesById = uniqueIndex(nodeSupplier.getWorkerNodes(), Node::getNodeIdentifier);

        ImmutableList.Builder<Node> bucketToNode = ImmutableList.builder();
        for (String nodeIdentifier : handle.getBucketToNode()) {
            Node node = nodesById.get(nodeIdentifier);
            if (node == null) {
                throw new PrestoException(NO_NODES_AVAILABLE, "Node for bucket is offline: " + nodeIdentifier);
            }
            bucketToNode.add(node);
        }
        return createBucketNodeMap(bucketToNode.build(), HARD_AFFINITY);
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning)
    {
        return value -> ((RaptorSplit) value).getBucketNumber().getAsInt();
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning, List<Type> partitionChannelTypes, int bucketCount)
    {
        return new RaptorBucketFunction(bucketCount, partitionChannelTypes);
    }

    @Override
    public int getBucketCount(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        RaptorPartitioningHandle handle = (RaptorPartitioningHandle) partitioningHandle;
        return handle.getBucketToNode().size();
    }
}
