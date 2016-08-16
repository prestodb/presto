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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.operator.BucketToPartitionFunction;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import javax.inject.Inject;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NodePartitioningManager
{
    private final NodeScheduler nodeScheduler;
    private final ConcurrentMap<String, ConnectorNodePartitioningProvider> partitioningProviders = new ConcurrentHashMap<>();

    @Inject
    public NodePartitioningManager(NodeScheduler nodeScheduler)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
    }

    public void addPartitioningProvider(String connectorId, ConnectorNodePartitioningProvider partitioningProvider)
    {
        checkArgument(
                partitioningProviders.putIfAbsent(connectorId, partitioningProvider) == null,
                "NodePartitioningProvider for connector '%s' is already registered",
                connectorId);
    }

    public Supplier<PartitionFunction> createRemotePartitionFunctionFactory(Session session, PartitioningScheme partitioningScheme)
    {
        checkArgument(partitioningScheme.getBucketToPartition().isPresent(), "Bucket to partition must be set before a remote partition function can be created");
        int[] bucketToPartition = partitioningScheme.getBucketToPartition().get();

        Supplier<BucketFunction> bucketFunctionSupplier;
        PartitioningHandle partitioningHandle = partitioningScheme.getPartitioning().getHandle();
        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            SystemPartitioningHandle systemPartitioningHandle = (SystemPartitioningHandle) partitioningHandle.getConnectorHandle();
            bucketFunctionSupplier = () -> systemPartitioningHandle.getBucketFunction(partitioningScheme.getHashColumn().isPresent(), bucketToPartition.length);
        }
        else {
            bucketFunctionSupplier = () -> getBucketFunction(session, partitioningHandle);
        }
        return new RemotePartitionFunctionSupplier(bucketFunctionSupplier, bucketToPartition);
    }

    private BucketFunction getBucketFunction(Session session, PartitioningHandle partitioningHandle)
    {
        checkArgument(!(partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle), "bucket function can not be fetch for system partitioning");

        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(partitioningHandle.getConnectorId().get());
        checkArgument(partitioningProvider != null, "No partitioning provider for connector %s", partitioningHandle.getConnectorId().get());

        return partitioningProvider.getBucketFunction(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioningHandle.getConnectorHandle());
    }

    public NodePartitionMap getNodePartitioningMap(Session session, PartitioningHandle partitioningHandle)
    {
        requireNonNull(session, "session is null");
        requireNonNull(partitioningHandle, "partitioningHandle is null");

        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            return ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getNodePartitionMap(nodeScheduler);
        }

        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(partitioningHandle.getConnectorId().get());
        checkArgument(partitioningProvider != null, "No partitioning provider for connector %s", partitioningHandle.getConnectorId().get());

        Map<Integer, Node> bucketToNode = partitioningProvider.getBucketToNode(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioningHandle.getConnectorHandle());
        checkArgument(bucketToNode != null, "No partition map %s", partitioningHandle);
        checkArgument(!bucketToNode.isEmpty(), "Partition map %s is empty", partitioningHandle);

        int bucketCount = bucketToNode.keySet().stream()
                .mapToInt(Integer::intValue)
                .max()
                .getAsInt() + 1;

        // safety check for crazy partitioning
        checkArgument(bucketCount < 1_000_000, "Too many buckets in partitioning: %s", bucketCount);

        int[] bucketToPartition = new int[bucketCount];
        BiMap<Node, Integer> nodeToPartition = HashBiMap.create();
        int nextPartitionId = 0;
        for (Entry<Integer, Node> entry : bucketToNode.entrySet()) {
            Integer partitionId = nodeToPartition.get(entry.getValue());
            if (partitionId == null) {
                partitionId = nextPartitionId++;
                nodeToPartition.put(entry.getValue(), partitionId);
            }
            bucketToPartition[entry.getKey()] = partitionId;
        }

        ToIntFunction<ConnectorSplit> splitBucketFunction = partitioningProvider.getSplitBucketFunction(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioningHandle.getConnectorHandle());
        checkArgument(splitBucketFunction != null, "No partitioning %s", partitioningHandle);

        return new NodePartitionMap(nodeToPartition.inverse(), bucketToPartition, split -> splitBucketFunction.applyAsInt(split.getConnectorSplit()));
    }

    private static class RemotePartitionFunctionSupplier
            implements Supplier<PartitionFunction>
    {
        private final Supplier<BucketFunction> bucketFunctionFactory;
        private final int[] bucketToPartition;

        public RemotePartitionFunctionSupplier(Supplier<BucketFunction> bucketFunctionFactory, int[] bucketToPartition)
        {
            this.bucketFunctionFactory = bucketFunctionFactory;
            this.bucketToPartition = bucketToPartition;
        }

        @Override
        public PartitionFunction get()
        {
            return new BucketToPartitionFunction(bucketFunctionFactory.get(), bucketToPartition);
        }
    }
}
