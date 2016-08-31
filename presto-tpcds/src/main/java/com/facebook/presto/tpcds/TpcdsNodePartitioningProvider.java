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
package com.facebook.presto.tpcds;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

import static com.facebook.presto.tpcds.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TpcdsNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;
    private final int splitsPerNode;

    public TpcdsNodePartitioningProvider(NodeManager nodeManager, int splitsPerNode)
    {
        requireNonNull(nodeManager, "nodeManager is null");
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");

        this.nodeManager = nodeManager;
        this.splitsPerNode = splitsPerNode;
    }

    @Override
    public Map<Integer, Node> getBucketToNode(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No TPCDS nodes available");

        // Split the data using split and skew by the number of nodes available.
        ImmutableMap.Builder<Integer, Node> bucketToNode = ImmutableMap.builder();
        int partNumber = 0;
        for (Node node : nodes) {
            for (int i = 0; i < splitsPerNode; i++) {
                bucketToNode.put(partNumber, node);
                partNumber++;
            }
        }
        return bucketToNode.build();
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> checkType(value, TpcdsSplit.class, "value").getPartNumber();
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        throw new NotImplementedException();
    }
}
