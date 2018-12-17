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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.raptorx.metadata.BucketNode;
import com.facebook.presto.raptorx.metadata.DistributionInfo;
import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.NodeSupplier;
import com.facebook.presto.raptorx.metadata.RaptorNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * Service to balance buckets across active Raptor storage nodes in a cluster.
 * <p> The objectives of this service are:
 * <ol>
 * <li> For a given distribution, each node should be allocated the same number of buckets
 * for a distribution. This enhances parallelism, and therefore query performance.
 * <li> The total disk utilization across the cluster should be balanced. This ensures that total
 * cluster storage capacity is maximized. Simply allocating the same number of buckets
 * to every node may not achieve this, as bucket sizes may vary dramatically across distributions.
 * </ol>
 * <p> This prioritizes query performance over total cluster storage capacity, and therefore may
 * produce a cluster state that is imbalanced in terms of disk utilization.
 */
public class BucketBalancer
{
    private static final Logger log = Logger.get(BucketBalancer.class);

    private final NodeSupplier nodeSupplier;
    private final Metadata metadata;
    private final boolean enabled;
    private final Duration interval;
    private final ScheduledExecutorService executor;

    private final AtomicBoolean started = new AtomicBoolean();

    private final CounterStat bucketsBalanced = new CounterStat();
    private final CounterStat jobErrors = new CounterStat();

    @Inject
    public BucketBalancer(
            NodeSupplier nodeSupplier,
            Metadata metadata,
            BucketBalancerConfig config)
    {
        this(nodeSupplier,
                metadata,
                config.isBalancerEnabled(),
                config.getBalancerInterval());
    }

    public BucketBalancer(
            NodeSupplier nodeSupplier,
            Metadata metadata,
            boolean enabled,
            Duration interval)
    {
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.enabled = enabled;
        this.interval = requireNonNull(interval, "interval is null");
        this.executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("bucket-balancer-"));
    }

    @PostConstruct
    public void start()
    {
        if (enabled && !started.getAndSet(true)) {
            executor.scheduleWithFixedDelay(this::runBalanceJob, interval.toMillis(), interval.toMillis(), MILLISECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Managed
    @Nested
    public CounterStat getBucketsBalanced()
    {
        return bucketsBalanced;
    }

    @Managed
    @Nested
    public CounterStat getJobErrors()
    {
        return jobErrors;
    }

    @Managed
    public void startBalanceJob()
    {
        executor.submit(this::runBalanceJob);
    }

    private void runBalanceJob()
    {
        try {
            balance();
        }
        catch (Throwable t) {
            log.error(t, "Error balancing buckets");
            jobErrors.update(1);
        }
    }

    @VisibleForTesting
    synchronized int balance()
    {
        log.info("Bucket balancer started. Computing assignments...");
        Multimap<Long, BucketAssignment> sourceToAssignmentChanges = computeAssignmentChanges(fetchClusterState());

        log.info("Moving buckets...");
        int moves = updateAssignments(sourceToAssignmentChanges);

        log.info("Bucket balancing finished. Moved %s buckets.", moves);
        return moves;
    }

    private static Multimap<Long, BucketAssignment> computeAssignmentChanges(ClusterState clusterState)
    {
        Multimap<Long, BucketAssignment> sourceToAllocationChanges = HashMultimap.create();

        Map<Long, Long> allocationBytes = new HashMap<>(clusterState.getAssignedBytes());
        Set<Long> activeNodes = clusterState.getActiveNodes();

        for (DistributionInfo distribution : clusterState.getDistributionAssignments().keySet()) {
            // number of buckets in this distribution assigned to a node
            Multiset<Long> allocationCounts = HashMultiset.create();
            Collection<BucketAssignment> distributionAssignments = clusterState.getDistributionAssignments().get(distribution);
            distributionAssignments.stream()
                    .map(BucketAssignment::getNodeId)
                    .forEach(allocationCounts::add);

            int currentMin = allocationBytes.keySet().stream()
                    .mapToInt(allocationCounts::count)
                    .min()
                    .getAsInt();
            int currentMax = allocationBytes.keySet().stream()
                    .mapToInt(allocationCounts::count)
                    .max()
                    .getAsInt();

            int numBuckets = distributionAssignments.size();
            int targetMin = (int) Math.floor((numBuckets * 1.0) / clusterState.getActiveNodes().size());
            int targetMax = (int) Math.ceil((numBuckets * 1.0) / clusterState.getActiveNodes().size());

            log.info("Distribution %s: Current bucket skew: min %s, max %s. Target bucket skew: min %s, max %s", distribution.getDistributionId(), currentMin, currentMax, targetMin, targetMax);

            for (Long source : ImmutableSet.copyOf(allocationCounts)) {
                List<BucketAssignment> existingAssignments = distributionAssignments.stream()
                        .filter(assignment -> assignment.getNodeId().equals(source))
                        .collect(toList());

                for (BucketAssignment existingAssignment : existingAssignments) {
                    if (activeNodes.contains(source) && allocationCounts.count(source) <= targetMin) {
                        break;
                    }

                    // identify nodes with bucket counts lower than the computed target, and greedily select from this set based on projected disk utilization.
                    // greediness means that this may produce decidedly non-optimal results if one looks at the global distribution of buckets->nodes.
                    // also, this assumes that nodes in a cluster have identical storage capacity
                    Long target = activeNodes.stream()
                            .filter(candidate -> !candidate.equals(source) && allocationCounts.count(candidate) < targetMax)
                            .sorted(comparingInt(allocationCounts::count))
                            .min(Comparator.comparingDouble(allocationBytes::get))
                            .orElseThrow(() -> new VerifyException("unable to find target for rebalancing"));

                    long bucketSize = clusterState.getDistributionBucketSize().get(distribution);

                    // only move bucket if it reduces imbalance
                    if (activeNodes.contains(source) && (allocationCounts.count(source) == targetMax && allocationCounts.count(target) == targetMin)) {
                        break;
                    }

                    allocationCounts.remove(source);
                    allocationCounts.add(target);
                    allocationBytes.compute(source, (k, v) -> v - bucketSize);
                    allocationBytes.compute(target, (k, v) -> v + bucketSize);

                    sourceToAllocationChanges.put(
                            existingAssignment.getNodeId(),
                            new BucketAssignment(existingAssignment.getDistributionId(), existingAssignment.getBucketNumber(), target));
                }
            }
        }

        return sourceToAllocationChanges;
    }

    private int updateAssignments(Multimap<Long, BucketAssignment> sourceToAllocationChanges)
    {
        // perform moves in decreasing order of source node total assigned buckets
        List<Long> sourceNodes = sourceToAllocationChanges.asMap().entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
                .map(Map.Entry::getKey)
                .collect(toList());

        int moves = 0;
        for (Long source : sourceNodes) {
            for (BucketAssignment reassignment : sourceToAllocationChanges.get(source)) {
                // todo: rate-limit new assignments
                metadata.updateBucketAssignment(reassignment.getDistributionId(), reassignment.getBucketNumber(), reassignment.getNodeId());
                bucketsBalanced.update(1);
                moves++;
                log.info("Distribution %s: Moved bucket %s from %s to %s",
                        reassignment.getDistributionId(),
                        reassignment.getBucketNumber(),
                        source,
                        reassignment.getNodeId());
            }
        }

        return moves;
    }

    @VisibleForTesting
    ClusterState fetchClusterState()
    {
        Set<Long> activeNodes = nodeSupplier.getWorkerNodes().stream()
                .map(RaptorNode::getNodeId)
                .collect(toSet());

        Map<Long, Long> assignedNodeSize = new HashMap<>(activeNodes.stream().collect(toMap(node -> node, node -> 0L)));
        ImmutableMultimap.Builder<DistributionInfo, BucketAssignment> distributionAssignments = ImmutableMultimap.builder();
        ImmutableMap.Builder<DistributionInfo, Long> distributionBucketSize = ImmutableMap.builder();

        for (DistributionInfo distribution : metadata.getActiveDistributions()) {
            long distributionSize = metadata.getDistributionSizeInBytes(distribution.getDistributionId());
            long bucketSize = (long) (1.0 * distributionSize) / distribution.getBucketCount();
            distributionBucketSize.put(distribution, bucketSize);

            for (BucketNode bucketNode : metadata.getBucketNodes(distribution.getDistributionId())) {
                Long node = bucketNode.getNodeId();
                distributionAssignments.put(distribution, new BucketAssignment(distribution.getDistributionId(), bucketNode.getBucketNumber(), node));
                assignedNodeSize.merge(node, bucketSize, Math::addExact);
            }
        }

        return new ClusterState(activeNodes, assignedNodeSize, distributionAssignments.build(), distributionBucketSize.build());
    }

    @VisibleForTesting
    static class ClusterState
    {
        private final Set<Long> activeNodes;
        private final Map<Long, Long> assignedBytes;
        private final Multimap<DistributionInfo, BucketAssignment> distributionAssignments;
        private final Map<DistributionInfo, Long> distributionBucketSize;

        public ClusterState(
                Set<Long> activeNodes,
                Map<Long, Long> assignedBytes,
                Multimap<DistributionInfo, BucketAssignment> distributionAssignments,
                Map<DistributionInfo, Long> distributionBucketSize)
        {
            this.activeNodes = ImmutableSet.copyOf(requireNonNull(activeNodes, "activeNodes is null"));
            this.assignedBytes = ImmutableMap.copyOf(requireNonNull(assignedBytes, "assignedBytes is null"));
            this.distributionAssignments = ImmutableMultimap.copyOf(requireNonNull(distributionAssignments, "distributionAssignments is null"));
            this.distributionBucketSize = ImmutableMap.copyOf(requireNonNull(distributionBucketSize, "distributionBucketSize is null"));
        }

        public Set<Long> getActiveNodes()
        {
            return activeNodes;
        }

        public Map<Long, Long> getAssignedBytes()
        {
            return assignedBytes;
        }

        public Multimap<DistributionInfo, BucketAssignment> getDistributionAssignments()
        {
            return distributionAssignments;
        }

        public Map<DistributionInfo, Long> getDistributionBucketSize()
        {
            return distributionBucketSize;
        }
    }

    @VisibleForTesting
    static class BucketAssignment
    {
        private final long distributionId;
        private final int bucketNumber;
        private final Long nodeId;

        public BucketAssignment(long distributionId, int bucketNumber, Long nodeId)
        {
            this.distributionId = distributionId;
            this.bucketNumber = bucketNumber;
            this.nodeId = requireNonNull(nodeId, "nodeIdentifier is null");
        }

        public long getDistributionId()
        {
            return distributionId;
        }

        public int getBucketNumber()
        {
            return bucketNumber;
        }

        public Long getNodeId()
        {
            return nodeId;
        }
    }
}
