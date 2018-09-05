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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ThriftNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private static final Logger log = Logger.get(ThriftNodePartitioningProvider.class);
    private final NodeManager nodeManager;

    @Inject
    public ThriftNodePartitioningProvider(
            @JsonProperty("nodeManager") NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @Override
    public Map<Integer, Node> getBucketToNode(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        ThriftPartitioningHandle handle = (ThriftPartitioningHandle) partitioningHandle;

        List<Node> nodes = shuffle(nodeManager.getRequiredWorkerNodes());
        // TODO: you're going to need to add the bucketedBy value in the ThriftPartitioningHandle, because since we now give a default value to bucketCount, we can no longer check it to see if we're actually bucketing stuff.
        log.info("In getBucketToNode method. Bucket count is %s", String.valueOf(handle.getBucketCount()));
        int bucketCount = handle.getBucketCount();
        ImmutableMap.Builder<Integer, Node> distribution = ImmutableMap.builder();
        for (int i = 0; i < bucketCount; i++) {
            // I'm going to give all the data to one node and see what happens.
            distribution.put(i, nodes.get(0)); //nodes.get(i % nodes.size()));
        }
        log.info("The nodes we're giving everything to is %s", nodes.get(0).getNodeIdentifier());
        return distribution.build();
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        // Reading splits by bucket is not supported
        return (split) -> 0;
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        return new ThriftBucketFunction(bucketCount);
    }

    @Override
    public List<ConnectorPartitionHandle> listPartitionHandles(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        ThriftPartitioningHandle handle = (ThriftPartitioningHandle) partitioningHandle;
        int bucketCount = handle.getBucketCount();
        return IntStream.range(0, bucketCount).mapToObj(ThriftPartitionHandle::new).collect(Collectors.toList());
    }

    private static <T> List<T> shuffle(Collection<T> items)
    {
        List<T> list = new ArrayList<>(items);
        Collections.shuffle(list);
        return list;
    }
}
