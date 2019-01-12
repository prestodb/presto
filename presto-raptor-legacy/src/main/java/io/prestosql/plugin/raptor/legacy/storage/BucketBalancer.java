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
package io.prestosql.plugin.raptor.legacy.storage;

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
import io.prestosql.plugin.raptor.legacy.NodeSupplier;
import io.prestosql.plugin.raptor.legacy.RaptorConnectorId;
import io.prestosql.plugin.raptor.legacy.backup.BackupService;
import io.prestosql.plugin.raptor.legacy.metadata.BucketNode;
import io.prestosql.plugin.raptor.legacy.metadata.Distribution;
import io.prestosql.plugin.raptor.legacy.metadata.ShardManager;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
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
    private final ShardManager shardManager;
    private final boolean enabled;
    private final Duration interval;
    private final boolean backupAvailable;
    private final boolean coordinator;
    private final ScheduledExecutorService executor;

    private final AtomicBoolean started = new AtomicBoolean();

    private final CounterStat bucketsBalanced = new CounterStat();
    private final CounterStat jobErrors = new CounterStat();

    @Inject
    public BucketBalancer(
            NodeManager nodeManager,
            NodeSupplier nodeSupplier,
            ShardManager shardManager,
            BucketBalancerConfig config,
            BackupService backupService,
            RaptorConnectorId connectorId)
    {
        this(nodeSupplier,
                shardManager,
                config.isBalancerEnabled(),
                config.getBalancerInterval(),
                backupService.isBackupAvailable(),
                nodeManager.getCurrentNode().isCoordinator(),
                connectorId.toString());
    }

    public BucketBalancer(
            NodeSupplier nodeSupplier,
            ShardManager shardManager,
            boolean enabled,
            Duration interval,
            boolean backupAvailable,
            boolean coordinator,
            String connectorId)
    {
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.enabled = enabled;
        this.interval = requireNonNull(interval, "interval is null");
        this.backupAvailable = backupAvailable;
        this.coordinator = coordinator;
        this.executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("bucket-balancer-" + connectorId));
    }

    @PostConstruct
    public void start()
    {
        if (enabled && backupAvailable && coordinator && !started.getAndSet(true)) {
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
        Multimap<String, BucketAssignment> sourceToAssignmentChanges = computeAssignmentChanges(fetchClusterState());

        log.info("Moving buckets...");
        int moves = updateAssignments(sourceToAssignmentChanges);

        log.info("Bucket balancing finished. Moved %s buckets.", moves);
        return moves;
    }

    private static Multimap<String, BucketAssignment> computeAssignmentChanges(ClusterState clusterState)
    {
        Multimap<String, BucketAssignment> sourceToAllocationChanges = HashMultimap.create();

        Map<String, Long> allocationBytes = new HashMap<>(clusterState.getAssignedBytes());
        Set<String> activeNodes = clusterState.getActiveNodes();

        for (Distribution distribution : clusterState.getDistributionAssignments().keySet()) {
            // number of buckets in this distribution assigned to a node
            Multiset<String> allocationCounts = HashMultiset.create();
            Collection<BucketAssignment> distributionAssignments = clusterState.getDistributionAssignments().get(distribution);
            distributionAssignments.stream()
                    .map(BucketAssignment::getNodeIdentifier)
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

            log.info("Distribution %s: Current bucket skew: min %s, max %s. Target bucket skew: min %s, max %s", distribution.getId(), currentMin, currentMax, targetMin, targetMax);

            for (String source : ImmutableSet.copyOf(allocationCounts)) {
                List<BucketAssignment> existingAssignments = distributionAssignments.stream()
                        .filter(assignment -> assignment.getNodeIdentifier().equals(source))
                        .collect(toList());

                for (BucketAssignment existingAssignment : existingAssignments) {
                    if (activeNodes.contains(source) && allocationCounts.count(source) <= targetMin) {
                        break;
                    }

                    // identify nodes with bucket counts lower than the computed target, and greedily select from this set based on projected disk utilization.
                    // greediness means that this may produce decidedly non-optimal results if one looks at the global distribution of buckets->nodes.
                    // also, this assumes that nodes in a cluster have identical storage capacity
                    String target = activeNodes.stream()
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
                            existingAssignment.getNodeIdentifier(),
                            new BucketAssignment(existingAssignment.getDistributionId(), existingAssignment.getBucketNumber(), target));
                }
            }
        }

        return sourceToAllocationChanges;
    }

    private int updateAssignments(Multimap<String, BucketAssignment> sourceToAllocationChanges)
    {
        // perform moves in decreasing order of source node total assigned buckets
        List<String> sourceNodes = sourceToAllocationChanges.asMap().entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
                .map(Map.Entry::getKey)
                .collect(toList());

        int moves = 0;
        for (String source : sourceNodes) {
            for (BucketAssignment reassignment : sourceToAllocationChanges.get(source)) {
                // todo: rate-limit new assignments
                shardManager.updateBucketAssignment(reassignment.getDistributionId(), reassignment.getBucketNumber(), reassignment.getNodeIdentifier());
                bucketsBalanced.update(1);
                moves++;
                log.info("Distribution %s: Moved bucket %s from %s to %s",
                        reassignment.getDistributionId(),
                        reassignment.getBucketNumber(),
                        source,
                        reassignment.getNodeIdentifier());
            }
        }

        return moves;
    }

    @VisibleForTesting
    ClusterState fetchClusterState()
    {
        Set<String> activeNodes = nodeSupplier.getWorkerNodes().stream()
                .map(Node::getNodeIdentifier)
                .collect(toSet());

        Map<String, Long> assignedNodeSize = new HashMap<>(activeNodes.stream().collect(toMap(node -> node, node -> 0L)));
        ImmutableMultimap.Builder<Distribution, BucketAssignment> distributionAssignments = ImmutableMultimap.builder();
        ImmutableMap.Builder<Distribution, Long> distributionBucketSize = ImmutableMap.builder();

        for (Distribution distribution : shardManager.getDistributions()) {
            long distributionSize = shardManager.getDistributionSizeInBytes(distribution.getId());
            long bucketSize = (long) (1.0 * distributionSize) / distribution.getBucketCount();
            distributionBucketSize.put(distribution, bucketSize);

            for (BucketNode bucketNode : shardManager.getBucketNodes(distribution.getId())) {
                String node = bucketNode.getNodeIdentifier();
                distributionAssignments.put(distribution, new BucketAssignment(distribution.getId(), bucketNode.getBucketNumber(), node));
                assignedNodeSize.merge(node, bucketSize, Math::addExact);
            }
        }

        return new ClusterState(activeNodes, assignedNodeSize, distributionAssignments.build(), distributionBucketSize.build());
    }

    @VisibleForTesting
    static class ClusterState
    {
        private final Set<String> activeNodes;
        private final Map<String, Long> assignedBytes;
        private final Multimap<Distribution, BucketAssignment> distributionAssignments;
        private final Map<Distribution, Long> distributionBucketSize;

        public ClusterState(
                Set<String> activeNodes,
                Map<String, Long> assignedBytes,
                Multimap<Distribution, BucketAssignment> distributionAssignments,
                Map<Distribution, Long> distributionBucketSize)
        {
            this.activeNodes = ImmutableSet.copyOf(requireNonNull(activeNodes, "activeNodes is null"));
            this.assignedBytes = ImmutableMap.copyOf(requireNonNull(assignedBytes, "assignedBytes is null"));
            this.distributionAssignments = ImmutableMultimap.copyOf(requireNonNull(distributionAssignments, "distributionAssignments is null"));
            this.distributionBucketSize = ImmutableMap.copyOf(requireNonNull(distributionBucketSize, "distributionBucketSize is null"));
        }

        public Set<String> getActiveNodes()
        {
            return activeNodes;
        }

        public Map<String, Long> getAssignedBytes()
        {
            return assignedBytes;
        }

        public Multimap<Distribution, BucketAssignment> getDistributionAssignments()
        {
            return distributionAssignments;
        }

        public Map<Distribution, Long> getDistributionBucketSize()
        {
            return distributionBucketSize;
        }
    }

    @VisibleForTesting
    static class BucketAssignment
    {
        private final long distributionId;
        private final int bucketNumber;
        private final String nodeIdentifier;

        public BucketAssignment(long distributionId, int bucketNumber, String nodeIdentifier)
        {
            this.distributionId = distributionId;
            this.bucketNumber = bucketNumber;
            this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null");
        }

        public long getDistributionId()
        {
            return distributionId;
        }

        public int getBucketNumber()
        {
            return bucketNumber;
        }

        public String getNodeIdentifier()
        {
            return nodeIdentifier;
        }
    }
}
