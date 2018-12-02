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
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.FixedBucketNodeMap;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.group.DynamicBucketNodeMap;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.BucketPartitionFunction;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.EmptySplit;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
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

    public void addPartitioningProvider(ConnectorId connectorId, ConnectorNodePartitioningProvider nodePartitioningProvider)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        checkArgument(partitioningProviders.putIfAbsent(connectorId, nodePartitioningProvider) == null,
                "NodePartitioningProvider for connector '%s' is already registered", connectorId);
    }

    public void removePartitioningProvider(ConnectorId connectorId)
    {
        partitioningProviders.remove(connectorId);
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
        return new BucketPartitionFunction(bucketFunction, partitioningScheme.getBucketToPartition().get());
    }

    public List<ConnectorPartitionHandle> listPartitionHandles(
            Session session,
            PartitioningHandle partitioningHandle)
    {
        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(partitioningHandle.getConnectorId().get());
        return partitioningProvider.listPartitionHandles(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioningHandle.getConnectorHandle());
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

        ConnectorBucketNodeMap connectorBucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);
        // safety check for crazy partitioning
        checkArgument(connectorBucketNodeMap.getBucketCount() < 1_000_000, "Too many buckets in partitioning: %s", connectorBucketNodeMap.getBucketCount());

        Map<Integer, Node> bucketToNode;
        if (connectorBucketNodeMap.hasFixedMapping()) {
            bucketToNode = connectorBucketNodeMap.getFixedMapping();
        }
        else {
            bucketToNode = createArbitraryBucketToNode(
                    new ArrayList<>(nodeScheduler.createNodeSelector(partitioningHandle.getConnectorId().get()).allNodes()),
                    connectorBucketNodeMap.getBucketCount());
        }

        int[] bucketToPartition = new int[connectorBucketNodeMap.getBucketCount()];
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

        return new NodePartitionMap(nodeToPartition.inverse(), bucketToPartition, getSplitToBucket(session, partitioningHandle));
    }

    public BucketNodeMap getBucketNodeMap(Session session, PartitioningHandle partitioningHandle, boolean preferDynamic)
    {
        ConnectorBucketNodeMap connectorBucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);

        if (connectorBucketNodeMap.hasFixedMapping()) {
            return new FixedBucketNodeMap(getSplitToBucket(session, partitioningHandle), connectorBucketNodeMap.getFixedMapping());
        }

        if (preferDynamic) {
            return new DynamicBucketNodeMap(getSplitToBucket(session, partitioningHandle), connectorBucketNodeMap.getBucketCount());
        }

        return new FixedBucketNodeMap(
                getSplitToBucket(session, partitioningHandle),
                createArbitraryBucketToNode(
                        new ArrayList<>(nodeScheduler.createNodeSelector(partitioningHandle.getConnectorId().get()).allNodes()),
                        connectorBucketNodeMap.getBucketCount()));
    }

    private ConnectorBucketNodeMap getConnectorBucketNodeMap(Session session, PartitioningHandle partitioningHandle)
    {
        checkArgument(!(partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle));

        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(partitioningHandle.getConnectorId().get());
        checkArgument(partitioningProvider != null, "No partitioning provider for connector %s", partitioningHandle.getConnectorId().get());

        ConnectorBucketNodeMap connectorBucketNodeMap = partitioningProvider.getBucketNodeMap(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioningHandle.getConnectorHandle());

        checkArgument(connectorBucketNodeMap != null, "No partition map %s", partitioningHandle);
        return connectorBucketNodeMap;
    }

    private ToIntFunction<Split> getSplitToBucket(Session session, PartitioningHandle partitioningHandle)
    {
        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(partitioningHandle.getConnectorId().get());
        checkArgument(partitioningProvider != null, "No partitioning provider for connector %s", partitioningHandle.getConnectorId().get());

        ToIntFunction<ConnectorSplit> splitBucketFunction = partitioningProvider.getSplitBucketFunction(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioningHandle.getConnectorHandle());
        checkArgument(splitBucketFunction != null, "No partitioning %s", partitioningHandle);

        return split -> {
            int bucket;
            if (split.getConnectorSplit() instanceof EmptySplit) {
                bucket = split.getLifespan().isTaskWide() ? 0 : split.getLifespan().getId();
            }
            else {
                bucket = splitBucketFunction.applyAsInt(split.getConnectorSplit());
            }
            if (!split.getLifespan().isTaskWide()) {
                checkArgument(split.getLifespan().getId() == bucket);
            }
            return bucket;
        };
    }

    private Map<Integer, Node> createArbitraryBucketToNode(List<Node> nodes, int bucketCount)
    {
        Collections.shuffle(nodes);

        ImmutableMap.Builder<Integer, Node> distribution = ImmutableMap.builder();
        for (int i = 0; i < bucketCount; i++) {
            distribution.put(i, nodes.get(i % nodes.size()));
        }
        return distribution.build();
    }
}
