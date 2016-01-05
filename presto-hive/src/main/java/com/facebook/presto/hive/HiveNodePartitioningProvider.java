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
package com.facebook.presto.hive;

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
import com.google.common.collect.Lists;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

import static com.facebook.presto.hive.util.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HiveNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final String connectorId;
    private final NodeManager nodeManager;

    @Inject
    public HiveNodePartitioningProvider(HiveConnectorId connectorId, NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        HivePartitioningHandle handle = checkType(partitioningHandle, HivePartitioningHandle.class, "partitioningHandle");
        List<HiveType> hiveTypes = handle.getHiveTypes();
        return new HiveBucketFunction(bucketCount, hiveTypes);
    }

    @Override
    public Map<Integer, Node> getBucketToNode(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        HivePartitioningHandle handle = checkType(partitioningHandle, HivePartitioningHandle.class, "partitioningHandle");

        Set<Node> nodesSet = nodeManager.getActiveDatasourceNodes(connectorId);
        checkState(!nodesSet.isEmpty(), "No %s nodes available", connectorId);
        List<Node> nodes = shuffle(nodesSet);

        int bucketCount = handle.getBucketCount();
        ImmutableMap.Builder<Integer, Node> distribution = ImmutableMap.builder();
        for (int i = 0; i < bucketCount; i++) {
            distribution.put(i, nodes.get(i % nodes.size()));
        }
        return distribution.build();
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> checkType(value, HiveSplit.class, "value").getBucketNumber().getAsInt();
    }

    private static <T> List<T> shuffle(Iterable<T> iterable)
    {
        List<T> list = Lists.newArrayList(iterable);
        Collections.shuffle(list);
        return list;
    }
}
