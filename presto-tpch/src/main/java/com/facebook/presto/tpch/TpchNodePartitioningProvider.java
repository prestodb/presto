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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

import static com.facebook.presto.tpch.Types.checkType;
import static com.google.common.base.Preconditions.checkState;

public class TpchNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final String connectorId;
    private final NodeManager nodeManager;

    public TpchNodePartitioningProvider(String connectorId, NodeManager nodeManager)
    {
        this.connectorId = connectorId;
        this.nodeManager = nodeManager;
    }

    @Override
    public Map<Integer, Node> getBucketToNode(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        TpchPartitioningHandle tpchPartitioningHandle = checkType(partitioningHandle, TpchPartitioningHandle.class, "partitioningHandle");
        Set<Node> nodes = nodeManager.getActiveDatasourceNodes(connectorId);
        checkState(!nodes.isEmpty(), "No TPCH nodes available");

        // Split the data using split and skew by the number of nodes available.
        ImmutableMap.Builder<Integer, Node> bucketToNode = ImmutableMap.builder();
        Iterator<Node> iterator = Iterables.cycle(nodes).iterator();
        for (int bucketNumber = 0; bucketNumber < tpchPartitioningHandle.getBucketCount(); bucketNumber++) {
            bucketToNode.put(bucketNumber, iterator.next());
        }
        return bucketToNode.build();
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> checkType(value, TpchSplit.class, "value").getPartNumber();
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        TpchPartitioningHandle tpchPartitioningHandle = checkType(partitioningHandle, TpchPartitioningHandle.class, "partitioningHandle");
        long totalRows = tpchPartitioningHandle.getTotalRows();
        int bucketCount = tpchPartitioningHandle.getBucketCount();
        long rowsPerBucket = totalRows / bucketCount;
        return new TpchBucketFunction(bucketCount, rowsPerBucket);
    }
}
