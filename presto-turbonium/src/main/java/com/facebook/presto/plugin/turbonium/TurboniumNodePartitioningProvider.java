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
package com.facebook.presto.plugin.turbonium;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public class TurboniumNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;

    @Inject
    public TurboniumNodePartitioningProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public Map<Integer, Node> getBucketToNode(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        // Copied from RaptorNodePartitioningProvider
        Map<String, Node> nodesById = uniqueIndex(nodeManager.getRequiredWorkerNodes(), Node::getNodeIdentifier);
        TurboniumPartitioningHandle handle = (TurboniumPartitioningHandle) partitioningHandle;
        ImmutableMap.Builder<Integer, Node> bucketToNode = ImmutableMap.builder();

        // create on part per node
        List<String> nodeList = handle.getBucketToNode();
        for (int partNumber = 0; partNumber < nodeList.size(); partNumber++) {
            String nodeId = nodeList.get(partNumber);
            Node node = nodesById.get(nodeId);
            if (node == null) {
                throw new PrestoException(NO_NODES_AVAILABLE, "Node for bucket is offline: " + nodeId);
            }
            bucketToNode.put(partNumber, node);
        }
        return bucketToNode.build();
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> ((TurboniumSplit) value).getBucket();
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes, int bucketCount)
    {
        return new RoundRobinBucketFunction(bucketCount);
    }

    private static class RoundRobinBucketFunction
            implements BucketFunction
    {
        private final int bucketCount;
        private int counter;

        public RoundRobinBucketFunction(int bucketCount)
        {
            checkArgument(bucketCount > 0, "bucketCount must be at least 1");
            this.bucketCount = bucketCount;
        }

        @Override
        public int getBucket(Page page, int position)
        {
            int bucket = counter % bucketCount;
            counter = (counter + 1) & 0x7fff_ffff;
            return bucket;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("bucketCount", bucketCount)
                    .toString();
        }
    }
}
