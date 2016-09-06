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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NodePartitioningManager
{
    private final NodeScheduler nodeScheduler;
    private final ConcurrentMap<ConnectorId, ConnectorNodePartitioningProvider> partitioningProviders = new ConcurrentHashMap<>();

    @Inject
    public NodePartitioningManager(NodeScheduler nodeScheduler)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
    }

    public void addPartitioningProvider(ConnectorId connectorId, ConnectorNodePartitioningProvider partitioningProvider)
    {
        checkArgument(partitioningProviders.putIfAbsent(connectorId, partitioningProvider) == null, "NodePartitioningProvider for connector '%s' is already registered", connectorId);
    }

    public PartitionFunction getPartitionFunction(
            Session session,
            PartitioningScheme partitioningScheme,
            List<Type> partitionChannelTypes)
    {
        Optional<int[]> bucketToPartition = partitioningScheme.getBucketToPartition();
        checkArgument(bucketToPartition.isPresent(), "Bucket to partition must be set before a partition function can be created");

        PartitioningHandle partitioningHandle = partitioningScheme.getPartitioning().getHandle();
        BucketFunction bucketFunction;
        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            checkArgument(partitioningScheme.getBucketToPartition().isPresent(), "Bucket to partition must be set before a partition function can be created");

            return ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getPartitionFunction(
                    partitionChannelTypes,
                    partitioningScheme.getHashColumn().isPresent(),
                    partitioningScheme.getBucketToPartition().get());
        }
        else {
            ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(partitioningHandle.getConnectorId().get());
            checkArgument(partitioningProvider != null, "No partitioning provider for connector %s", partitioningHandle.getConnectorId().get());

            bucketFunction = partitioningProvider.getBucketFunction(
                    partitioningHandle.getTransactionHandle().orElse(null),
                    session.toConnectorSession(),
                    partitioningHandle.getConnectorHandle(),
                    partitionChannelTypes,
                    bucketToPartition.get().length);

            checkArgument(bucketFunction != null, "No function %s", partitioningHandle);
        }
        return new PartitionFunction(bucketFunction, partitioningScheme.getBucketToPartition().get());
    }

    public NodePartitionMap getNodePartitioningMap(Session session, PartitioningHandle partitioningHandle)
    {
        requireNonNull(session, "session is null");
        requireNonNull(partitioningHandle, "partitioningHandle is null");

        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            return ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getNodePartitionMap(session, nodeScheduler);
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
}
