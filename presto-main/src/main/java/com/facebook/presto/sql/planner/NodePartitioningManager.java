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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.FixedBucketNodeMap;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.group.DynamicBucketNodeMap;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.BucketPartitionFunction;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.split.EmptySplit;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.getMaxTasksPerStage;
import static com.facebook.presto.metadata.InternalNode.NodeStatus.DEAD;
import static com.facebook.presto.spi.StandardErrorCode.NODE_SELECTION_NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NodePartitioningManager
{
    private final NodeScheduler nodeScheduler;
    private final PartitioningProviderManager partitioningProviderManager;
    private final NodeSelectionStats nodeSelectionStats;

    @Inject
    public NodePartitioningManager(NodeScheduler nodeScheduler, PartitioningProviderManager partitioningProviderManager, NodeSelectionStats nodeSelectionStats)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.partitioningProviderManager = requireNonNull(partitioningProviderManager, "partitioningProviderManager is null");
        this.nodeSelectionStats = requireNonNull(nodeSelectionStats, "nodeSelectionStats is null");
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
            ConnectorNodePartitioningProvider partitioningProvider = partitioningProviderManager.getPartitioningProvider(partitioningHandle.getConnectorId().get());

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
        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviderManager.getPartitioningProvider(partitioningHandle.getConnectorId().get());
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

        ConnectorId connectorId = partitioningHandle.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("No connector ID for partitioning handle: " + partitioningHandle));
        ConnectorBucketNodeMap connectorBucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);
        // safety check for crazy partitioning
        checkArgument(connectorBucketNodeMap.getBucketCount() < 1_000_000, "Too many buckets in partitioning: %s", connectorBucketNodeMap.getBucketCount());

        List<InternalNode> bucketToNode;
        NodeSelectionStrategy nodeSelectionStrategy = connectorBucketNodeMap.getNodeSelectionStrategy();
        boolean cacheable;
        switch (nodeSelectionStrategy) {
            case HARD_AFFINITY:
                bucketToNode = getFixedMapping(connectorBucketNodeMap);
                cacheable = false;
                break;
            case SOFT_AFFINITY:
                bucketToNode = getFixedMapping(connectorBucketNodeMap);
                cacheable = true;
                break;
            case NO_PREFERENCE:
                bucketToNode = createArbitraryBucketToNode(
                        nodeScheduler.createNodeSelector(session, connectorId).selectRandomNodes(getMaxTasksPerStage(session)),
                        connectorBucketNodeMap.getBucketCount());
                cacheable = false;
                break;
            default:
                throw new PrestoException(NODE_SELECTION_NOT_SUPPORTED, format("Unsupported node selection strategy %s", nodeSelectionStrategy));
        }

        int[] bucketToPartition = new int[connectorBucketNodeMap.getBucketCount()];
        BiMap<InternalNode, Integer> nodeToPartition = HashBiMap.create();
        int nextPartitionId = 0;
        for (int bucket = 0; bucket < bucketToNode.size(); bucket++) {
            InternalNode node = bucketToNode.get(bucket);
            Integer partitionId = nodeToPartition.get(node);
            if (partitionId == null) {
                partitionId = nextPartitionId++;
                nodeToPartition.put(node, partitionId);
            }
            bucketToPartition[bucket] = partitionId;
        }

        List<InternalNode> partitionToNode = IntStream.range(0, nodeToPartition.size())
                .mapToObj(partitionId -> nodeToPartition.inverse().get(partitionId))
                .collect(toImmutableList());

        return new NodePartitionMap(partitionToNode, bucketToPartition, getSplitToBucket(session, partitioningHandle), cacheable);
    }

    public BucketNodeMap getBucketNodeMap(Session session, PartitioningHandle partitioningHandle, boolean preferDynamic)
    {
        ConnectorBucketNodeMap connectorBucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);

        NodeSelectionStrategy nodeSelectionStrategy = connectorBucketNodeMap.getNodeSelectionStrategy();
        switch (nodeSelectionStrategy) {
            case HARD_AFFINITY:
                return new FixedBucketNodeMap(getSplitToBucket(session, partitioningHandle), getFixedMapping(connectorBucketNodeMap), false);
            case SOFT_AFFINITY:
                if (preferDynamic) {
                    return new DynamicBucketNodeMap(getSplitToBucket(session, partitioningHandle), connectorBucketNodeMap.getBucketCount(), getFixedMapping(connectorBucketNodeMap));
                }
                return new FixedBucketNodeMap(getSplitToBucket(session, partitioningHandle), getFixedMapping(connectorBucketNodeMap), true);
            case NO_PREFERENCE:
                if (preferDynamic) {
                    return new DynamicBucketNodeMap(getSplitToBucket(session, partitioningHandle), connectorBucketNodeMap.getBucketCount());
                }
                return new FixedBucketNodeMap(
                        getSplitToBucket(session, partitioningHandle),
                        createArbitraryBucketToNode(
                                nodeScheduler.createNodeSelector(session, partitioningHandle.getConnectorId().get()).selectRandomNodes(getMaxTasksPerStage(session)),
                                connectorBucketNodeMap.getBucketCount()),
                        false);
            default:
                throw new PrestoException(NODE_SELECTION_NOT_SUPPORTED, format("Unsupported node selection strategy %s", nodeSelectionStrategy));
        }
    }

    private static List<InternalNode> getFixedMapping(ConnectorBucketNodeMap connectorBucketNodeMap)
    {
        return connectorBucketNodeMap.getFixedMapping().stream()
                .map(InternalNode.class::cast)
                .collect(toImmutableList());
    }

    private ConnectorBucketNodeMap getConnectorBucketNodeMap(Session session, PartitioningHandle partitioningHandle)
    {
        checkArgument(!(partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle));
        ConnectorId connectorId = partitioningHandle.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("No connector ID for partitioning handle: " + partitioningHandle));
        List<Node> nodes = getNodes(session, connectorId);

        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviderManager.getPartitioningProvider(partitioningHandle.getConnectorId().get());

        ConnectorBucketNodeMap connectorBucketNodeMap = partitioningProvider.getBucketNodeMap(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(partitioningHandle.getConnectorId().get()),
                partitioningHandle.getConnectorHandle(),
                nodes);

        checkArgument(connectorBucketNodeMap != null, "No partition map %s", partitioningHandle);
        return connectorBucketNodeMap;
    }

    private ToIntFunction<Split> getSplitToBucket(Session session, PartitioningHandle partitioningHandle)
    {
        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviderManager.getPartitioningProvider(partitioningHandle.getConnectorId().get());

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

    private static List<InternalNode> createArbitraryBucketToNode(List<InternalNode> nodes, int bucketCount)
    {
        List<InternalNode> shuffledNodes = new ArrayList<>(nodes);
        Collections.shuffle(shuffledNodes);

        ImmutableList.Builder<InternalNode> distribution = ImmutableList.builderWithExpectedSize(bucketCount);
        for (int i = 0; i < bucketCount; i++) {
            distribution.add(shuffledNodes.get(i % shuffledNodes.size()));
        }
        return distribution.build();
    }

    public List<Node> getNodes(Session session, ConnectorId connectorId)
    {
        // Nodes returned by the node selector are already sorted based on nodeIdentifier. No need to sort again
        List<InternalNode> allNodes = nodeScheduler.createNodeSelector(session, connectorId).getAllNodes();

        ImmutableList.Builder<Node> nodeBuilder = ImmutableList.builder();
        int nodeCount = allNodes.size();
        for (int i = 0; i < nodeCount; i++) {
            InternalNode node = allNodes.get(i);
            if (node.getNodeStatus() == DEAD) {
                // Replace dead nodes with the first alive node to the right of it in the sorted node list
                int index = (i + 1) % nodeCount;
                while (node.getNodeStatus() == DEAD && index < nodeCount) {
                    node = allNodes.get(index);
                    index = (index + 1) % nodeCount;
                }
                nodeSelectionStats.incrementBucketedNonAliveNodeReplacedCount();
            }
            nodeBuilder.add(node);
        }
        return nodeBuilder.build();
    }
}
