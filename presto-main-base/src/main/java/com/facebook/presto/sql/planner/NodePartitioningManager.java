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
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
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
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.split.EmptySplit;
import com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitioning;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.SystemSessionProperties.getMaxTasksPerStage;
import static com.facebook.presto.metadata.InternalNode.NodeStatus.DEAD;
import static com.facebook.presto.spi.StandardErrorCode.NODE_SELECTION_NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.min;
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
        else if (partitioningHandle.getConnectorHandle() instanceof MergePartitioningHandle) {
            MergePartitioningHandle handle = (MergePartitioningHandle) partitioningHandle.getConnectorHandle();
            return handle.getPartitionFunction(
                    (scheme, types) -> getPartitionFunction(session, scheme, types),
                    partitionChannelTypes, bucketToPartition.get());
        }
        else {
            ConnectorNodePartitioningProvider partitioningProvider = partitioningProviderManager.getPartitioningProvider(partitioningHandle.getConnectorId().get());

            bucketFunction = partitioningProvider.getBucketFunction(
                    partitioningHandle.getTransactionHandle().orElse(null),
                    session.toConnectorSession(partitioningHandle.getConnectorId().get()),
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
        return getNodePartitioningMap(session, partitioningHandle, Optional.empty());
    }

    /**
     * This method is recursive for MergePartitioningHandle. It caches the node mappings
     * to ensure that both the insert and update layouts use the same mapping.
     */
    public NodePartitionMap getNodePartitioningMap(Session session, PartitioningHandle partitioningHandle, Optional<Predicate<Node>> nodePredicate)
    {
        requireNonNull(session, "session is null");
        requireNonNull(partitioningHandle, "partitioningHandle is null");

        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            // TODO #20578: The next commented line is the original code. The following one is an alternative that I need to check if is valid.
            // return ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getNodePartitionMap(session, nodeScheduler, nodePredicate);
            return systemNodePartitionMap(session, partitioningHandle, nodePredicate);
        }

        if (partitioningHandle.getConnectorHandle() instanceof MergePartitioningHandle) {
            MergePartitioningHandle mergeHandle = (MergePartitioningHandle) partitioningHandle.getConnectorHandle();
            return mergeHandle.getNodePartitioningMap(handle -> getNodePartitioningMap(session, handle, nodePredicate));
        }

        ConnectorId connectorId = partitioningHandle.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("No connector ID for partitioning handle: " + partitioningHandle));

        Optional<ConnectorBucketNodeMap> optionalMap = getConnectorBucketNodeMap(session, partitioningHandle, nodePredicate);
        if (!optionalMap.isPresent()) {
            return systemNodePartitionMap(session, FIXED_HASH_DISTRIBUTION, nodePredicate);
        }
        ConnectorBucketNodeMap connectorBucketNodeMap = optionalMap.get();

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
        Optional<ConnectorBucketNodeMap> connectorBucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle, Optional.empty());

        int bucketCount = getBucketCount(session, partitioningHandle, connectorBucketNodeMap, preferDynamic);

        // TODO #20578: WIP - This method is under development. Unsafe ".get()" method call.
        NodeSelectionStrategy nodeSelectionStrategy = connectorBucketNodeMap.get().getNodeSelectionStrategy();
        switch (nodeSelectionStrategy) {
            case HARD_AFFINITY:
                return new FixedBucketNodeMap(getSplitToBucket(session, partitioningHandle), getFixedMapping(connectorBucketNodeMap.get()), false);
            case SOFT_AFFINITY:
                if (preferDynamic) {
                    return new DynamicBucketNodeMap(getSplitToBucket(session, partitioningHandle), bucketCount, getFixedMapping(connectorBucketNodeMap.get()));
                }
                return new FixedBucketNodeMap(getSplitToBucket(session, partitioningHandle), getFixedMapping(connectorBucketNodeMap.get()), true);
            case NO_PREFERENCE:
                if (preferDynamic) {
                    return new DynamicBucketNodeMap(getSplitToBucket(session, partitioningHandle), bucketCount);
                }

                return new FixedBucketNodeMap(
                        getSplitToBucket(session, partitioningHandle),
                        createArbitraryBucketToNode(
                                nodeScheduler.createNodeSelector(session, partitioningHandle.getConnectorId().get()).selectRandomNodes(getMaxTasksPerStage(session)),
                                bucketCount),
                        false);
            default:
                throw new PrestoException(NODE_SELECTION_NOT_SUPPORTED, format("Unsupported node selection strategy %s", nodeSelectionStrategy));
        }
    }

    private Integer getBucketCount(
            Session session,
            PartitioningHandle partitioningHandle,
            Optional<ConnectorBucketNodeMap> connectorBucketNodeMap,
            boolean preferDynamic)
    {
        Optional<Integer> bucketCount = connectorBucketNodeMap.map(ConnectorBucketNodeMap::getBucketCount);

        return bucketCount.orElseGet(() -> preferDynamic ?
                getNodeCount(session, partitioningHandle) : getAllNodes(session, partitioningHandle).size());
    }

    public int getNodeCount(Session session, PartitioningHandle partitioningHandle)
    {
        return getAllNodes(session, partitioningHandle).size();
    }

    private List<InternalNode> getAllNodes(Session session, PartitioningHandle partitioningHandle)
    {
        return nodeScheduler.createNodeSelector(session, partitioningHandle.getConnectorId().get()).getAllNodes();
    }

    private static List<InternalNode> getFixedMapping(ConnectorBucketNodeMap connectorBucketNodeMap)
    {
        return connectorBucketNodeMap.getFixedMapping().stream()
                .map(InternalNode.class::cast)
                .collect(toImmutableList());
    }

    private Optional<ConnectorBucketNodeMap> getConnectorBucketNodeMap(Session session, PartitioningHandle partitioningHandle, Optional<Predicate<Node>> nodePredicate)
    {
        checkArgument(!(partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle));
        ConnectorId connectorId = partitioningHandle.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("No connector ID for partitioning handle: " + partitioningHandle));
        List<Node> nodes = getNodes(session, connectorId, nodePredicate);

        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviderManager.getPartitioningProvider(partitioningHandle.getConnectorId().get());

        return partitioningProvider.getBucketNodeMap(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(partitioningHandle.getConnectorId().get()),
                partitioningHandle.getConnectorHandle(),
                nodes);

        // TODO #20578: Now this method returns an Optional, so the following lines are not necessary.
//        checkArgument(connectorBucketNodeMap != null, "No partition map %s", partitioningHandle);
//        return connectorBucketNodeMap;
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

    public List<Node> getNodes(Session session, ConnectorId connectorId, Optional<Predicate<Node>> nodePredicate)
    {
        // Nodes returned by the node selector are already sorted based on nodeIdentifier. No need to sort again
        List<InternalNode> allNodes = nodeScheduler.createNodeSelector(session, connectorId, nodePredicate).getAllNodes();

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

    private NodePartitionMap systemNodePartitionMap(Session session, PartitioningHandle partitioningHandle, Optional<Predicate<Node>> nodePredicate)
    {
        SystemPartitioning partitioning = ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getPartitioning();

        // TODO #20578: Be careful with the "partitioningHandle.getConnectorId().orElse(null)". Evaluate possible side effects.
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, partitioningHandle.getConnectorId().orElse(null), nodePredicate);

        List<InternalNode> nodes;
        if (partitioning == SystemPartitioning.COORDINATOR_ONLY) {
            nodes = ImmutableList.of(nodeSelector.selectCurrentNode());
        }
        else if (partitioning == SystemPartitioning.SINGLE) {
            nodes = nodeSelector.selectRandomNodes(1);
        }
        else if (partitioning == SystemPartitioning.FIXED) {
            nodes = nodeSelector.selectRandomNodes(min(getHashPartitionCount(session), getMaxTasksPerStage(session)));
        }
        else {
            throw new IllegalArgumentException("Unsupported plan distribution " + partitioning);
        }
        checkCondition(!nodes.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");

        return new NodePartitionMap(nodes, split -> {
            throw new UnsupportedOperationException("System distribution does not support source splits");
        });
    }
}
