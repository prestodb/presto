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

import com.facebook.presto.common.ConnectorSession;
import com.facebook.presto.common.PrestoException;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;
import java.util.function.ToIntFunction;

import static com.facebook.presto.common.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class BlackHoleNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;

    public BlackHoleNodePartitioningProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Node> sortedNodes)
    {
        // create one bucket per node
        return createBucketNodeMap(nodeManager.getRequiredWorkerNodes().size());
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
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes, int bucketCount)
    {
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

    @Override
    public int getBucketCount(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        // create one bucket per node
        return nodeManager.getRequiredWorkerNodes().size();
    }
}
