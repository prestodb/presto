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
package com.facebook.presto.execution;

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.dispatcher.NoOpQueryManager;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.ModularHashingNodeProvider;
import com.facebook.presto.execution.scheduler.NetworkLocation;
import com.facebook.presto.execution.scheduler.NetworkLocationCache;
import com.facebook.presto.execution.scheduler.NetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.SplitPlacementResult;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelectorConfig;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.spi.ttl.ConfidenceBasedTtlInfo;
import com.facebook.presto.spi.ttl.NodeInfo;
import com.facebook.presto.spi.ttl.NodeTtl;
import com.facebook.presto.spi.ttl.NodeTtlFetcherFactory;
import com.facebook.presto.spi.ttl.TestingNodeTtlFetcherFactory;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.ttl.nodettlfetchermanagers.ConfidenceBasedNodeTtlFetcherManager;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManagerConfig;
import com.facebook.presto.ttl.nodettlfetchermanagers.ThrowingNodeTtlFetcherManager;
import com.facebook.presto.util.FinalizerService;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SystemSessionProperties.MAX_UNACKNOWLEDGED_SPLITS_PER_TASK;
import static com.facebook.presto.SystemSessionProperties.RESOURCE_AWARE_SCHEDULING_STRATEGY;
import static com.facebook.presto.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static com.facebook.presto.execution.scheduler.NodeSelectionHashStrategy.CONSISTENT_HASHING;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestNodeScheduler
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("connector_id");
    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;
    private InMemoryNodeManager nodeManager;
    private NodeSchedulerConfig nodeSchedulerConfig;
    private NodeScheduler nodeScheduler;
    private NodeSelector nodeSelector;
    private Map<InternalNode, RemoteTask> taskMap;
    private ExecutorService remoteTaskExecutor;
    private ScheduledExecutorService remoteTaskScheduledExecutor;
    private Session session;

    @BeforeMethod
    public void setUp()
    {
        session = TestingSession.testSessionBuilder().build();
        finalizerService = new FinalizerService();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        nodeManager = new InMemoryNodeManager();

        ImmutableList.Builder<InternalNode> nodeBuilder = ImmutableList.builder();
        nodeBuilder.add(new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        List<InternalNode> nodes = nodeBuilder.build();
        nodeManager.addNode(CONNECTOR_ID, nodes);
        nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                nodeManager,
                new NodeSelectionStats(),
                nodeSchedulerConfig,
                nodeTaskMap,
                new ThrowingNodeTtlFetcherManager(),
                new NoOpQueryManager(),
                new SimpleTtlNodeSelectorConfig());
        // contents of taskMap indicate the node-task map for the current stage
        taskMap = new HashMap<>();
        nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID);
        remoteTaskExecutor = newCachedThreadPool(daemonThreadsNamed("remoteTaskExecutor-%s"));
        remoteTaskScheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("remoteTaskScheduledExecutor-%s"));

        finalizerService.start();
    }

    @AfterMethod
    public void tearDown()
    {
        remoteTaskExecutor.shutdown();
        remoteTaskScheduledExecutor.shutdown();
        finalizerService.destroy();
        nodeSchedulerConfig = null;
        nodeScheduler = null;
        nodeSelector = null;
    }

    private class TestingQueryManager
            extends NoOpQueryManager
    {
        private Duration executionTime;

        private BasicQueryStats getBasicQueryStats(Duration executionTime)
        {
            Duration defaultDuration = Duration.valueOf("5m");
            return new BasicQueryStats(
                    0,
                    0,
                    defaultDuration,
                    defaultDuration,
                    defaultDuration,
                    executionTime,
                    defaultDuration,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    DataSize.valueOf("1MB"),
                    0,
                    0,
                    0,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    ImmutableSet.of(),
                    DataSize.valueOf("1MB"),
                    OptionalDouble.empty());
        }

        private BasicQueryInfo getBasicQueryInfo(Duration executionTime)
        {
            return new BasicQueryInfo(
                    session.getQueryId(),
                    session.toSessionRepresentation(),
                    Optional.empty(),
                    QueryState.RUNNING,
                    null,
                    true,
                    URI.create("http://127.0.0.1:55"),
                    "",
                    getBasicQueryStats(executionTime),
                    null,
                    null,
                    null,
                    Optional.empty(),
                    ImmutableList.of(),
                    Optional.empty());
        }

        @Override
        public BasicQueryInfo getQueryInfo(QueryId queryId)
        {
            return getBasicQueryInfo(executionTime);
        }

        public void setExecutionTime(Duration executionTime)
        {
            this.executionTime = executionTime;
        }
    }

    @Test
    public void testScheduleLocal()
    {
        Split split = new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitLocal());
        Set<Split> splits = ImmutableSet.of(split);

        Map.Entry<InternalNode, Split> assignment = Iterables.getOnlyElement(nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments().entries());
        ModularHashingNodeProvider modularHashingNodeProvider = new ModularHashingNodeProvider(nodeSelector.getAllNodes());
        assertEquals(assignment.getKey().getHostAndPort(), split.getPreferredNodes((key) -> modularHashingNodeProvider.get(key, 3)).get(0));
        assertEquals(assignment.getValue(), split);
    }

    @Test(timeOut = 60 * 1000)
    public void testTopologyAwareScheduling()
            throws Exception
    {
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        ImmutableList.Builder<InternalNode> nodeBuilder = ImmutableList.builder();
        nodeBuilder.add(new InternalNode("node1", URI.create("http://host1.rack1:11"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new InternalNode("node2", URI.create("http://host2.rack1:12"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new InternalNode("node3", URI.create("http://host3.rack2:13"), NodeVersion.UNKNOWN, false));
        List<InternalNode> nodes = nodeBuilder.build();
        nodeManager.addNode(CONNECTOR_ID, nodes);

        // contents of taskMap indicate the node-task map for the current stage
        Map<InternalNode, RemoteTask> taskMap = new HashMap<>();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(25)
                .setIncludeCoordinator(false)
                .setNetworkTopology("test")
                .setMaxPendingSplitsPerTask(20);

        TestNetworkTopology topology = new TestNetworkTopology();
        NetworkLocationCache locationCache = new NetworkLocationCache(topology)
        {
            @Override
            public NetworkLocation get(HostAddress host)
            {
                // Bypass the cache for workers, since we only look them up once and they would all be unresolved otherwise
                if (host.getHostText().startsWith("host")) {
                    return topology.locate(host);
                }
                else {
                    return super.get(host);
                }
            }
        };
        NodeScheduler nodeScheduler = new NodeScheduler(locationCache, topology, nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap, new Duration(5, SECONDS), new ThrowingNodeTtlFetcherManager(), new NoOpQueryManager(), new SimpleTtlNodeSelectorConfig());
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID);

        // Fill up the nodes with non-local data
        ImmutableSet.Builder<Split> nonRackLocalBuilder = ImmutableSet.builder();
        for (int i = 0; i < (25 + 11) * 3; i++) {
            nonRackLocalBuilder.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(HostAddress.fromParts("data.other_rack", 1))));
        }
        Set<Split> nonRackLocalSplits = nonRackLocalBuilder.build();
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(nonRackLocalSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        int task = 0;
        for (InternalNode node : assignments.keySet()) {
            TaskId taskId = new TaskId("test", 1, 0, task, 0);
            task++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, ImmutableList.copyOf(assignments.get(node)), nodeTaskMap.createTaskStatsTracker(node, taskId));
            remoteTask.startSplits(25);
            nodeTaskMap.addTask(node, remoteTask);
            taskMap.put(node, remoteTask);
        }
        // Continue assigning to fill up part of the queue
        nonRackLocalSplits = Sets.difference(nonRackLocalSplits, new HashSet<>(assignments.values()));
        assignments = nodeSelector.computeAssignments(nonRackLocalSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        for (InternalNode node : assignments.keySet()) {
            RemoteTask remoteTask = taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments.get(node))
                    .build());
        }
        nonRackLocalSplits = Sets.difference(nonRackLocalSplits, new HashSet<>(assignments.values()));
        // Check that 3 of the splits were rejected, since they're non-local
        assertEquals(nonRackLocalSplits.size(), 3);

        // Assign rack-local splits
        ImmutableSet.Builder<Split> rackLocalSplits = ImmutableSet.builder();
        HostAddress dataHost1 = HostAddress.fromParts("data.rack1", 1);
        HostAddress dataHost2 = HostAddress.fromParts("data.rack2", 1);
        for (int i = 0; i < 6 * 2; i++) {
            rackLocalSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(dataHost1)));
        }
        for (int i = 0; i < 6; i++) {
            rackLocalSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(dataHost2)));
        }
        assignments = nodeSelector.computeAssignments(rackLocalSplits.build(), ImmutableList.copyOf(taskMap.values())).getAssignments();
        for (InternalNode node : assignments.keySet()) {
            RemoteTask remoteTask = taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments.get(node))
                    .build());
        }
        Set<Split> unassigned = Sets.difference(rackLocalSplits.build(), new HashSet<>(assignments.values()));
        // Compute the assignments a second time to account for the fact that some splits may not have been assigned due to asynchronous
        // loading of the NetworkLocationCache
        boolean cacheRefreshed = false;
        while (!cacheRefreshed) {
            cacheRefreshed = true;
            if (locationCache.get(dataHost1).equals(ROOT_LOCATION)) {
                cacheRefreshed = false;
            }
            if (locationCache.get(dataHost2).equals(ROOT_LOCATION)) {
                cacheRefreshed = false;
            }
            MILLISECONDS.sleep(10);
        }
        assignments = nodeSelector.computeAssignments(unassigned, ImmutableList.copyOf(taskMap.values())).getAssignments();
        for (InternalNode node : assignments.keySet()) {
            RemoteTask remoteTask = taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments.get(node))
                    .build());
        }
        unassigned = Sets.difference(unassigned, new HashSet<>(assignments.values()));
        assertEquals(unassigned.size(), 3);
        ModularHashingNodeProvider modularHashingNodeProvider = new ModularHashingNodeProvider(nodeSelector.getAllNodes());
        int rack1 = 0;
        int rack2 = 0;
        for (Split split : unassigned) {
            String rack = topology.locate(split.getPreferredNodes((key) -> modularHashingNodeProvider.get(key, 2)).get(0)).getSegments().get(0);
            switch (rack) {
                case "rack1":
                    rack1++;
                    break;
                case "rack2":
                    rack2++;
                    break;
                default:
                    fail();
            }
        }
        assertEquals(rack1, 2);
        assertEquals(rack2, 1);

        // Assign local splits
        ImmutableSet.Builder<Split> localSplits = ImmutableSet.builder();
        localSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(HostAddress.fromParts("host1.rack1", 1))));
        localSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(HostAddress.fromParts("host2.rack1", 1))));
        localSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(HostAddress.fromParts("host3.rack2", 1))));
        assignments = nodeSelector.computeAssignments(localSplits.build(), ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertEquals(assignments.size(), 3);
        assertEquals(assignments.keySet().size(), 3);
    }

    @Test
    public void testTtlAwareScheduling()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        InternalNode node1 = new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false);
        InternalNode node3 = new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false);
        List<InternalNode> nodes = ImmutableList.of(node1, node2, node3);
        nodeManager.addNode(CONNECTOR_ID, nodes);
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        Instant currentInstant = Instant.now();
        NodeTtl ttl1 = new NodeTtl(ImmutableSet.of(
                new ConfidenceBasedTtlInfo(currentInstant.plus(5, ChronoUnit.MINUTES).getEpochSecond(), 100)));
        NodeTtl ttl2 = new NodeTtl(ImmutableSet.of(
                new ConfidenceBasedTtlInfo(currentInstant.plus(30, ChronoUnit.MINUTES).getEpochSecond(), 100)));
        NodeTtl ttl3 = new NodeTtl(ImmutableSet.of(
                new ConfidenceBasedTtlInfo(currentInstant.plus(2, ChronoUnit.HOURS).getEpochSecond(), 100)));
        Map<NodeInfo, NodeTtl> nodeToTtl = ImmutableMap.of(
                new NodeInfo(node1.getNodeIdentifier(), node1.getHost()),
                ttl1,
                new NodeInfo(node2.getNodeIdentifier(), node2.getHost()),
                ttl2,
                new NodeInfo(node3.getNodeIdentifier(), node3.getHost()),
                ttl3);

        ConfidenceBasedNodeTtlFetcherManager nodeTtlFetcherManager = new ConfidenceBasedNodeTtlFetcherManager(
                nodeManager,
                new NodeSchedulerConfig(),
                new NodeTtlFetcherManagerConfig());
        NodeTtlFetcherFactory nodeTtlFetcherFactory = new TestingNodeTtlFetcherFactory(nodeToTtl);
        nodeTtlFetcherManager.addNodeTtlFetcherFactory(nodeTtlFetcherFactory);
        nodeTtlFetcherManager.load(nodeTtlFetcherFactory.getName(), ImmutableMap.of());
        nodeTtlFetcherManager.refreshTtlInfo();

        TestingQueryManager queryManager = new TestingQueryManager();
        NodeScheduler fallbackEnabledNodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                nodeManager,
                new NodeSelectionStats(),
                nodeSchedulerConfig,
                nodeTaskMap,
                nodeTtlFetcherManager,
                queryManager,
                new SimpleTtlNodeSelectorConfig().setFallbackToSimpleNodeSelection(true));

        NodeScheduler fallbackDisabledNodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                nodeManager,
                new NodeSelectionStats(),
                nodeSchedulerConfig,
                nodeTaskMap,
                nodeTtlFetcherManager,
                queryManager,
                new SimpleTtlNodeSelectorConfig().setFallbackToSimpleNodeSelection(false));

        // Query is estimated to take 20 mins and has been executing for 3 mins, i.e, 17 mins left
        // So only node2 and node3 have enough TTL to run additional work
        Session session = sessionWithTtlAwareSchedulingStrategyAndEstimatedExecutionTime(new Duration(20, TimeUnit.MINUTES));
        NodeSelector nodeSelector = fallbackEnabledNodeScheduler.createNodeSelector(session, CONNECTOR_ID);
        queryManager.setExecutionTime(new Duration(3, TimeUnit.MINUTES));
        assertEquals(ImmutableSet.copyOf(nodeSelector.selectRandomNodes(3)), ImmutableSet.of(node2, node3));

        // Query is estimated to take 5 hours and has been executing for 1 hour, i.e, 4 hours left
        // No nodes will have enough TTL, so we fall back to simple node selection as per the config above
        session = sessionWithTtlAwareSchedulingStrategyAndEstimatedExecutionTime(new Duration(5, TimeUnit.HOURS));
        nodeSelector = fallbackEnabledNodeScheduler.createNodeSelector(session, CONNECTOR_ID);
        queryManager.setExecutionTime(new Duration(1, TimeUnit.HOURS));
        assertEquals(ImmutableSet.copyOf(nodeSelector.selectRandomNodes(3)), ImmutableSet.of(node1, node2, node3));

        // Query is estimated to take 20 mins and has been executing for 3 mins, i.e, 17 minutes left
        // So only node2 and node3 have enough TTL to run additional work, however, they are also in the excluded nodes list
        // So no nodes are selected
        session = sessionWithTtlAwareSchedulingStrategyAndEstimatedExecutionTime(new Duration(5, TimeUnit.HOURS));
        nodeSelector = fallbackDisabledNodeScheduler.createNodeSelector(session, CONNECTOR_ID);
        queryManager.setExecutionTime(new Duration(1, TimeUnit.HOURS));
        assertEquals(ImmutableSet.copyOf(nodeSelector.selectRandomNodes(3, ImmutableSet.of(node2, node3))), ImmutableSet.of());

        // Query is estimated to take 1 hour and has been executing for 45 mins, i.e, 15 mins left
        // So only node2 and node3 have enough TTL to work on new splits
        session = sessionWithTtlAwareSchedulingStrategyAndEstimatedExecutionTime(new Duration(1, TimeUnit.HOURS));
        nodeSelector = fallbackEnabledNodeScheduler.createNodeSelector(session, CONNECTOR_ID);
        queryManager.setExecutionTime(new Duration(45, TimeUnit.MINUTES));
        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 2; i++) {
            splits.add(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()));
        }
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertEquals(assignments.size(), 2);
        assertTrue(assignments.keySet().contains(node2));
        assertTrue(assignments.keySet().contains(node3));

        // Query is estimated to take 5 hours and has been executing for 1 hour, i.e, 4 hours left
        // No nodes will have enough TTL, so we fall back to simple node selection as per the config above
        session = sessionWithTtlAwareSchedulingStrategyAndEstimatedExecutionTime(new Duration(5, TimeUnit.HOURS));
        nodeSelector = fallbackEnabledNodeScheduler.createNodeSelector(session, CONNECTOR_ID);
        queryManager.setExecutionTime(new Duration(1, TimeUnit.HOURS));
        splits.clear();
        for (int i = 0; i < 3; i++) {
            splits.add(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()));
        }

        assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertEquals(assignments.size(), 3);
        assertEquals(assignments.keySet().size(), 3);
        assertTrue(assignments.keySet().contains(node1));
        assertTrue(assignments.keySet().contains(node2));
        assertTrue(assignments.keySet().contains(node3));

        // Query is estimated to take 1 hour and has been executing for 20 mins, i.e, 40 mins left
        // So only node3 has enough TTL to work on new splits
        session = sessionWithTtlAwareSchedulingStrategyAndEstimatedExecutionTime(new Duration(1, TimeUnit.HOURS));
        nodeSelector = fallbackEnabledNodeScheduler.createNodeSelector(session, CONNECTOR_ID);
        queryManager.setExecutionTime(new Duration(20, TimeUnit.MINUTES));
        splits.clear();
        for (int i = 0; i < 2; i++) {
            splits.add(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()));
        }

        assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertEquals(assignments.size(), 2);
        assertEquals(assignments.keySet().size(), 1);
        assertTrue(assignments.keySet().contains(node3));
    }

    @Test
    public void testScheduleRemote()
    {
        Set<Split> splits = new HashSet<>();
        splits.add(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()));
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertEquals(assignments.size(), 1);
    }

    @Test
    public void testBasicAssignment()
    {
        // One split for each node
        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            splits.add(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()));
        }
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertEquals(assignments.entries().size(), 3);
        for (InternalNode node : nodeManager.getActiveConnectorNodes(CONNECTOR_ID)) {
            assertTrue(assignments.keySet().contains(node));
        }
    }

    @Test
    public void testBasicAssignmentMaxUnacknowledgedSplitsPerTask()
    {
        // Use non-default max unacknowledged splits per task
        nodeSelector = nodeScheduler.createNodeSelector(sessionWithMaxUnacknowledgedSplitsPerTask(1), CONNECTOR_ID, Integer.MAX_VALUE);
        // One split for each node, and one extra split that can't be placed
        int nodeCount = nodeManager.getActiveConnectorNodes(CONNECTOR_ID).size();
        int splitCount = nodeCount + 1;
        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()));
        }
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertEquals(assignments.entries().size(), nodeCount);
        for (InternalNode node : nodeManager.getActiveConnectorNodes(CONNECTOR_ID)) {
            assertTrue(assignments.keySet().contains(node));
        }
    }

    @Test
    public void testAffinityAssignmentNotSupported()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        LegacyNetworkTopology legacyNetworkTopology = new LegacyNetworkTopology();
        NodeScheduler nodeScheduler = new NodeScheduler(new NetworkLocationCache(legacyNetworkTopology), legacyNetworkTopology, nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap, new Duration(0, SECONDS), new ThrowingNodeTtlFetcherManager(), new NoOpQueryManager(), new SimpleTtlNodeSelectorConfig());
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 2);

        Set<Split> splits = new HashSet<>();

        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(HostAddress.fromString("127.0.0.1:10"))));
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(HostAddress.fromString("127.0.0.1:10"))));
        SplitPlacementResult splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        Set<InternalNode> internalNodes = splitPlacementResult.getAssignments().keySet();
        // Split doesn't support affinity schedule, fall back to random schedule
        assertEquals(internalNodes.size(), 2);
    }

    @Test
    public void testAffinityAssignmentWithModularHashing()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap, new ThrowingNodeTtlFetcherManager(), new NoOpQueryManager(), new SimpleTtlNodeSelectorConfig());
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 3);

        Set<Split> splits = new HashSet<>();

        // Adding one more split (1 % 3 = 1), 1 splits will be distributed to 1 nodes
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestAffinitySplitRemote(1)));
        SplitPlacementResult splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        Set<InternalNode> internalNodes = splitPlacementResult.getAssignments().keySet();
        assertEquals(internalNodes.size(), 1);

        // Adding one more split (2 % 3 = 2), 2 splits will be distributed to 2 nodes
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestAffinitySplitRemote(2)));
        splitPlacementResult = nodeSelector.computeAssignments(splits, getRemoteTableScanTask(splitPlacementResult));
        Set<InternalNode> internalNodesSecondCall = splitPlacementResult.getAssignments().keySet();
        assertEquals(internalNodesSecondCall.size(), 2);

        // adding one more split(4 % 3 = 1) that will fall into the same slots, 3 splits will be distributed to 2 nodes still
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestAffinitySplitRemote(4)));
        splitPlacementResult = nodeSelector.computeAssignments(splits, getRemoteTableScanTask(splitPlacementResult));
        assertEquals(splitPlacementResult.getAssignments().keySet().size(), 2);

        // adding one more split(3 % 3 = 0) that will fall into different slots, 3 splits will be distributed to 3 nodes
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestAffinitySplitRemote(3)));
        splitPlacementResult = nodeSelector.computeAssignments(splits, getRemoteTableScanTask(splitPlacementResult));
        assertEquals(splitPlacementResult.getAssignments().keySet().size(), 3);
    }

    @Test
    public void testAffinityAssignmentWithConsistentHashing()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setNodeSelectionHashStrategy(CONSISTENT_HASHING)
                .setMinVirtualNodeCount(3)
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap, new ThrowingNodeTtlFetcherManager(), new NoOpQueryManager(), new SimpleTtlNodeSelectorConfig());
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 3);

        Set<Split> splits = new HashSet<>();
        IntStream.range(0, 10).forEach(i -> splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestAffinitySplitRemote(i))));

        InternalNode node1 = new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false);
        InternalNode node3 = new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false);
        InternalNode node4 = new InternalNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);

        // In setup node 1-3 are added to node manager
        SplitPlacementResult splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        assertEquals(splitPlacementResult.getAssignments().keySet().size(), 3);
        // node1: split 1, 3, 4, 5, 6, 7, 8, 9
        Collection<ConnectorSplit> node1Splits = splitPlacementResult.getAssignments().get(node1).stream().map(Split::getConnectorSplit).collect(toImmutableSet());
        // node2: 0
        Collection<Object> node2Splits = splitPlacementResult.getAssignments().get(node2).stream().map(Split::getConnectorSplit).collect(toImmutableSet());
        // node3: split 2
        Collection<Object> node3Splits = splitPlacementResult.getAssignments().get(node3).stream().map(Split::getConnectorSplit).collect(toImmutableSet());

        // Scheduling the same splits on the same set of nodes should give the same assignment
        nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 3);
        splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        assertEquals(splitPlacementResult.getAssignments().get(node1).stream().map(Split::getConnectorSplit).collect(toImmutableSet()), node1Splits);
        assertEquals(splitPlacementResult.getAssignments().get(node2).stream().map(Split::getConnectorSplit).collect(toImmutableSet()), node2Splits);
        assertEquals(splitPlacementResult.getAssignments().get(node3).stream().map(Split::getConnectorSplit).collect(toImmutableSet()), node3Splits);

        // Adding node4. Node4 is hashed in between node3 and node1, and all splits previously assigned to node1 are now assigned to node4. Assignment to node2 and node3 should not change.
        nodeManager.addNode(CONNECTOR_ID, node4);
        nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 3);
        splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        assertEquals(splitPlacementResult.getAssignments().keySet().size(), 3);
        assertEquals(splitPlacementResult.getAssignments().get(node1), ImmutableList.of());
        assertEquals(splitPlacementResult.getAssignments().get(node2).stream().map(Split::getConnectorSplit).collect(toImmutableSet()), node2Splits);
        assertEquals(splitPlacementResult.getAssignments().get(node3).stream().map(Split::getConnectorSplit).collect(toImmutableSet()), node3Splits);
        assertEquals(splitPlacementResult.getAssignments().get(node4).stream().map(Split::getConnectorSplit).collect(toImmutableSet()), node1Splits);
    }

    @Test
    public void testAffinityAssignmentWithConsistentHashingWithVirtualNodes()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setNodeSelectionHashStrategy(CONSISTENT_HASHING)
                .setMinVirtualNodeCount(5)
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap, new ThrowingNodeTtlFetcherManager(), new NoOpQueryManager(), new SimpleTtlNodeSelectorConfig());
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 3);

        Set<Split> splits = new HashSet<>();
        IntStream.range(0, 10).forEach(i -> splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestAffinitySplitRemote(i))));

        InternalNode node1 = new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false);
        InternalNode node3 = new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false);
        InternalNode node4 = new InternalNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);

        // In setup node 1-3 are added to node manager
        // consistent hashing ring for nodes:
        // entry0 (-1907319920): node2
        // entry1 (-1028466245): node3
        // entry2 ( -546736344): node2
        // entry3 ( 1127574531): node3
        // entry4 ( 1166245243): node1
        // entry5 ( 2145381619): node1
        SplitPlacementResult splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        // hashing value for splits:
        // 0: -1962219106 -> entry0 -> node2
        // 1:   145569539 -> entry3 -> node3
        // 2: -1599101205 -> entry1 -> node3
        // 3:  -165119218 -> entry3 -> node3
        // 4:  1142216720 -> entry4 -> node1
        // 5:  1347620135 -> entry5 -> node1
        // 6:  1232195252 -> entry5 -> node1
        // 7:   427886318 -> entry3 -> node3
        // 8:  1469878697 -> entry5 -> node1
        // 9:   296801082 -> entry3 -> node3
        assertEquals(splitPlacementResult.getAssignments().keySet().size(), 3);
        // node1: split 4, 5, 6, 8
        Collection<ConnectorSplit> node1Splits = splitPlacementResult.getAssignments().get(node1).stream().map(Split::getConnectorSplit).collect(toImmutableSet());
        // node2: split 0
        Collection<Object> node2Splits = splitPlacementResult.getAssignments().get(node2).stream().map(Split::getConnectorSplit).collect(toImmutableSet());
        // node3: split 1, 2, 3, 7, 9
        Collection<Object> node3Splits = splitPlacementResult.getAssignments().get(node3).stream().map(Split::getConnectorSplit).collect(toImmutableSet());

        // Scheduling the same splits on the same set of nodes should give the same assignment
        nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 3);
        splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        assertEquals(splitPlacementResult.getAssignments().get(node1).stream().map(Split::getConnectorSplit).collect(toImmutableSet()), node1Splits);
        assertEquals(splitPlacementResult.getAssignments().get(node2).stream().map(Split::getConnectorSplit).collect(toImmutableSet()), node2Splits);
        assertEquals(splitPlacementResult.getAssignments().get(node3).stream().map(Split::getConnectorSplit).collect(toImmutableSet()), node3Splits);

        // Adding node4, consistent hashing ring for nodes:
        // entry0 (-1907319920): node2
        // entry1 (-1616890413): node4
        // entry2 (-1028466245): node3
        // entry3 ( -546736344): node2
        // entry4 ( 1127574531): node3
        // entry5 ( 1166245243): node1
        // entry6 ( 1691928386): node4
        // entry7 ( 2145381619): node1
        nodeManager.addNode(CONNECTOR_ID, node4);
        nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 3);
        splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        // hashing value for splits:
        // 0: -1962219106 -> entry0 -> node2
        // 1:   145569539 -> entry4 -> node3
        // 2: -1599101205 -> entry2 -> node3
        // 3:  -165119218 -> entry4 -> node3
        // 4:  1142216720 -> entry5 -> node1
        // 5:  1347620135 -> entry6 -> node4
        // 6:  1232195252 -> entry6 -> node4
        // 7:   427886318 -> entry4 -> node3
        // 8:  1469878697 -> entry6 -> node4
        // 9:   296801082 -> entry4 -> node3
        assertEquals(splitPlacementResult.getAssignments().keySet().size(), 4);
        assertEquals(splitPlacementResult.getAssignments().get(node1).stream().map(Split::getConnectorSplit).map(ConnectorSplit::getSplitIdentifier).collect(toImmutableSet()), ImmutableSet.of(4));
        assertEquals(splitPlacementResult.getAssignments().get(node2).stream().map(Split::getConnectorSplit).collect(toImmutableSet()), node2Splits);
        assertEquals(splitPlacementResult.getAssignments().get(node3).stream().map(Split::getConnectorSplit).map(ConnectorSplit::getSplitIdentifier).collect(toImmutableSet()), ImmutableSet.of(1, 2, 3, 7, 9));
        assertEquals(splitPlacementResult.getAssignments().get(node4).stream().map(Split::getConnectorSplit).map(ConnectorSplit::getSplitIdentifier).collect(toImmutableSet()), ImmutableSet.of(5, 6, 8));
    }

    @Test
    public void testHardAffinityAssignment()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap, new ThrowingNodeTtlFetcherManager(), new NoOpQueryManager(), new SimpleTtlNodeSelectorConfig());
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 3);

        Set<Split> splits = new HashSet<>();

        // Adding one more split (1 % 3 = 1), 1 splits will be distributed to 1 nodes
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestHardAffinitySplitRemote()));
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestHardAffinitySplitRemote()));
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestHardAffinitySplitRemote()));
        SplitPlacementResult splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        for (Split split : splitPlacementResult.getAssignments().values()) {
            assertTrue(split.getSplitContext().isCacheable());
        }
    }

    @Test
    public void testMaxSplitsPerNode()
    {
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();

        InternalNode newNode = new InternalNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, newNode);

        ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            initialSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        }

        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        // Max out number of splits on node
        TaskId taskId1 = new TaskId("test", 1, 0, 1, 0);
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1, newNode, initialSplits.build(), nodeTaskMap.createTaskStatsTracker(newNode, taskId1));
        nodeTaskMap.addTask(newNode, remoteTask1);

        TaskId taskId2 = new TaskId("test", 1, 0, 2, 0);
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(taskId2, newNode, initialSplits.build(), nodeTaskMap.createTaskStatsTracker(newNode, taskId2));
        nodeTaskMap.addTask(newNode, remoteTask2);

        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        }
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();

        // no split should be assigned to the newNode, as it already has maxNodeSplits assigned to it
        assertFalse(assignments.keySet().contains(newNode));

        remoteTask1.abort();
        remoteTask2.abort();

        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(newNode), PartitionedSplitsInfo.forZeroSplits());
    }

    @Test
    public void testMaxSplitsPerNodePerTask()
    {
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();

        InternalNode newNode = new InternalNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, newNode);

        ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
        for (int i = 0; i < 20; i++) {
            initialSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        }

        List<RemoteTask> tasks = new ArrayList<>();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        for (InternalNode node : nodeManager.getActiveConnectorNodes(CONNECTOR_ID)) {
            // Max out number of splits on node
            TaskId taskId = new TaskId("test", 1, 0, 1, 0);
            RemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, initialSplits.build(), nodeTaskMap.createTaskStatsTracker(node, taskId));
            nodeTaskMap.addTask(node, remoteTask);
            tasks.add(remoteTask);
        }

        TaskId taskId = new TaskId("test", 1, 0, 2, 0);
        RemoteTask newRemoteTask = remoteTaskFactory.createTableScanTask(taskId, newNode, initialSplits.build(), nodeTaskMap.createTaskStatsTracker(newNode, taskId));
        // Max out pending splits on new node
        taskMap.put(newNode, newRemoteTask);
        nodeTaskMap.addTask(newNode, newRemoteTask);
        tasks.add(newRemoteTask);

        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        }
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();

        // no split should be assigned to the newNode, as it already has
        // maxSplitsPerNode + maxSplitsPerNodePerTask assigned to it
        assertEquals(assignments.keySet().size(), 3); // Splits should be scheduled on the other three nodes
        assertFalse(assignments.keySet().contains(newNode)); // No splits scheduled on the maxed out node

        for (RemoteTask task : tasks) {
            task.abort();
        }
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(newNode), PartitionedSplitsInfo.forZeroSplits());
    }

    @Test
    public void testMaxUnacknowledgedSplitsPerTask()
    {
        int maxUnacknowledgedSplitsPerTask = 5;
        nodeSelector = nodeScheduler.createNodeSelector(sessionWithMaxUnacknowledgedSplitsPerTask(maxUnacknowledgedSplitsPerTask), CONNECTOR_ID, Integer.MAX_VALUE);

        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
        for (int i = 0; i < maxUnacknowledgedSplitsPerTask; i++) {
            initialSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        }

        List<InternalNode> nodes = new ArrayList<>();
        List<MockRemoteTaskFactory.MockRemoteTask> tasks = new ArrayList<>();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        int counter = 1;
        for (InternalNode node : nodeManager.getActiveConnectorNodes(CONNECTOR_ID)) {
            // Max out number of unacknowledged splits on each task
            TaskId taskId = new TaskId("test", 1, 0, counter, 0);
            counter++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, initialSplits.build(), nodeTaskMap.createTaskStatsTracker(node, taskId));
            nodeTaskMap.addTask(node, remoteTask);
            remoteTask.setMaxUnacknowledgedSplits(maxUnacknowledgedSplitsPerTask);
            remoteTask.setUnacknowledgedSplits(maxUnacknowledgedSplitsPerTask);
            nodes.add(node);
            tasks.add(remoteTask);
        }

        // One split per node
        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < nodes.size(); i++) {
            splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        }
        SplitPlacementResult splitPlacements = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(tasks));
        // No splits should have been placed, max unacknowledged was already reached
        assertEquals(splitPlacements.getAssignments().size(), 0);
        assertFalse(splitPlacements.getBlocked().isDone());

        // Unblock one task
        MockRemoteTaskFactory.MockRemoteTask taskOne = tasks.get(0);
        taskOne.finishSplits(1);
        taskOne.setUnacknowledgedSplits(taskOne.getUnacknowledgedPartitionedSplitCount() - 1);
        assertTrue(splitPlacements.getBlocked().isDone());

        // Attempt to schedule again, only the node with the unblocked task should be chosen
        splitPlacements = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(tasks));
        assertEquals(splitPlacements.getAssignments().size(), 1);
        assertTrue(splitPlacements.getAssignments().keySet().contains(nodes.get(0)));

        // Make the first node appear to have no splits, unacknowledged splits alone should force the splits to be spread across nodes
        taskOne.clearSplits();
        // Give all tasks with room for 1 unacknowledged split
        tasks.forEach(task -> task.setUnacknowledgedSplits(maxUnacknowledgedSplitsPerTask - 1));

        splitPlacements = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(tasks));
        // One split placed on each node
        assertEquals(splitPlacements.getAssignments().size(), nodes.size());
        assertTrue(splitPlacements.getAssignments().keySet().containsAll(nodes));
    }

    @Test
    public void testTaskCompletion()
            throws Exception
    {
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        InternalNode chosenNode = Iterables.get(nodeManager.getActiveConnectorNodes(CONNECTOR_ID), 0);
        TaskId taskId = new TaskId("test", 1, 0, 1, 0);
        RemoteTask remoteTask = remoteTaskFactory.createTableScanTask(
                taskId,
                chosenNode,
                ImmutableList.of(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote())),
                nodeTaskMap.createTaskStatsTracker(chosenNode, taskId));
        nodeTaskMap.addTask(chosenNode, remoteTask);
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), standardWeightSplitsInfo(1));
        remoteTask.abort();
        MILLISECONDS.sleep(100); // Sleep until cache expires
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), PartitionedSplitsInfo.forZeroSplits());

        remoteTask.abort();
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), PartitionedSplitsInfo.forZeroSplits());
    }

    @Test
    public void testSplitCount()
    {
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        InternalNode chosenNode = Iterables.get(nodeManager.getActiveConnectorNodes(CONNECTOR_ID), 0);

        TaskId taskId1 = new TaskId("test", 1, 0, 1, 0);
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1,
                chosenNode,
                ImmutableList.of(
                        new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()),
                        new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote())),
                nodeTaskMap.createTaskStatsTracker(chosenNode, taskId1));

        TaskId taskId2 = new TaskId("test", 1, 0, 2, 0);
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(
                taskId2,
                chosenNode,
                ImmutableList.of(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote())),
                nodeTaskMap.createTaskStatsTracker(chosenNode, taskId2));

        nodeTaskMap.addTask(chosenNode, remoteTask1);
        nodeTaskMap.addTask(chosenNode, remoteTask2);
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), standardWeightSplitsInfo(3));

        remoteTask1.abort();
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), standardWeightSplitsInfo(1));
        remoteTask2.abort();
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), PartitionedSplitsInfo.forZeroSplits());
    }

    @Test
    public void testMoreSplitsAssignedWhenSplitsWeightsAreSmall()
    {
        int standardSplitsPerNode = nodeSchedulerConfig.getMaxSplitsPerNode();
        int standardPendingSplitsPerTask = nodeSchedulerConfig.getMaxPendingSplitsPerTask();
        int fullyLoadedStandardSplitCount = standardSplitsPerNode + standardPendingSplitsPerTask;
        long weightLimitPerNode = SplitWeight.rawValueForStandardSplitCount(standardSplitsPerNode);
        long weightLimitPendingPerTask = SplitWeight.rawValueForStandardSplitCount(standardPendingSplitsPerTask);
        long fullyLoadedStandardSplitWeight = weightLimitPerNode + weightLimitPendingPerTask;

        // Single worker node
        nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 1);
        InternalNode workerNode = nodeSelector.selectRandomNodes(1).get(0);
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);

        TaskId taskId = new TaskId("test", 1, 0, 1, 0);
        MockRemoteTaskFactory.MockRemoteTask task = remoteTaskFactory.createTableScanTask(taskId, workerNode, ImmutableList.of(), nodeTaskMap.createTaskStatsTracker(workerNode, taskId));

        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        ImmutableSet.Builder<Split> splitsBuilder = ImmutableSet.builderWithExpectedSize(fullyLoadedStandardSplitCount * 2);
        // Create 2x more splits than the standard split count limit, at 1/2 the standard weight
        SplitWeight halfWeight = SplitWeight.fromProportion(0.5);
        for (int i = 0; i < fullyLoadedStandardSplitCount * 2; i++) {
            splitsBuilder.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(halfWeight)));
        }
        Set<Split> splits = splitsBuilder.build();
        // Verify we arrived at the exact weight limit
        assertEquals(SplitWeight.rawValueSum(splits, Split::getSplitWeight), fullyLoadedStandardSplitWeight);

        // Node assignment limit met
        SplitPlacementResult result = nodeSelector.computeAssignments(splits, ImmutableList.of(task));
        assertEquals(result.getAssignments().get(workerNode).size(), standardSplitsPerNode * 2);
        assertEquals(SplitWeight.rawValueSum(result.getAssignments().get(workerNode), Split::getSplitWeight), weightLimitPerNode);

        // Mark all splits as running
        task.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                .putAll(new PlanNodeId("sourceId"), result.getAssignments().get(workerNode))
                .build());
        task.startSplits(result.getAssignments().get(workerNode).size());

        // Per task pending splits limit met
        Set<Split> remainingSplits = Sets.difference(splits, ImmutableSet.copyOf(result.getAssignments().get(workerNode)));
        SplitPlacementResult secondResults = nodeSelector.computeAssignments(remainingSplits, ImmutableList.of(task));
        assertEquals(secondResults.getAssignments().get(workerNode).size(), standardPendingSplitsPerTask * 2);
        assertEquals(SplitWeight.rawValueSum(secondResults.getAssignments().get(workerNode), Split::getSplitWeight), weightLimitPendingPerTask);
        task.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                .putAll(new PlanNodeId("sourceId"), secondResults.getAssignments().get(workerNode))
                .build());

        assertEquals(
                nodeTaskMap.getPartitionedSplitsOnNode(workerNode),
                // 2x fully loaded standard count, full weight limit reached
                PartitionedSplitsInfo.forSplitCountAndWeightSum(fullyLoadedStandardSplitCount * 2, fullyLoadedStandardSplitWeight));

        // No more splits assigned when full
        SplitPlacementResult resultWhenFull = nodeSelector.computeAssignments(ImmutableSet.of(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote())), ImmutableList.of(task));
        assertTrue(resultWhenFull.getAssignments().isEmpty());
    }

    @Test
    public void testCpuUsage()
    {
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        InternalNode chosenNode = Iterables.get(nodeManager.getActiveConnectorNodes(CONNECTOR_ID), 0);

        TaskId taskId1 = new TaskId("test", 1, 0, 1, 0);
        List<Split> splits = ImmutableList.of(
                new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()),
                new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()));
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1,
                chosenNode,
                splits,
                nodeTaskMap.createTaskStatsTracker(chosenNode, taskId1));

        TaskId taskId2 = new TaskId("test", 1, 0, 2, 0);
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(
                taskId2,
                chosenNode,
                ImmutableList.of(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote())),
                nodeTaskMap.createTaskStatsTracker(chosenNode, taskId2));

        nodeTaskMap.addTask(chosenNode, remoteTask1);
        nodeTaskMap.addTask(chosenNode, remoteTask2);

        remoteTask2.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                .putAll(new PlanNodeId("sourceId"), splits)
                .build());

        assertEquals(nodeTaskMap.getNodeCpuUtilizationPercentage(chosenNode), 100D);
        remoteTask1.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                .putAll(new PlanNodeId("sourceId"), splits)
                .build());
        assertEquals(nodeTaskMap.getNodeCpuUtilizationPercentage(chosenNode), 200D);
        remoteTask1.abort();
        assertEquals(nodeTaskMap.getNodeCpuUtilizationPercentage(chosenNode), 100D);
        remoteTask2.abort();
        assertEquals(nodeTaskMap.getNodeCpuUtilizationPercentage(chosenNode), 0D);
    }

    @Test
    public void testMemoryUsage()
    {
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        InternalNode chosenNode = Iterables.get(nodeManager.getActiveConnectorNodes(CONNECTOR_ID), 0);

        TaskId taskId1 = new TaskId("test", 1, 0, 1, 0);
        List<Split> splits = ImmutableList.of(
                new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()),
                new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()));
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1,
                chosenNode,
                splits,
                nodeTaskMap.createTaskStatsTracker(chosenNode, taskId1));

        TaskId taskId2 = new TaskId("test", 1, 0, 2, 0);
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(
                taskId2,
                chosenNode,
                ImmutableList.of(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote())),
                nodeTaskMap.createTaskStatsTracker(chosenNode, taskId2));

        nodeTaskMap.addTask(chosenNode, remoteTask1);
        nodeTaskMap.addTask(chosenNode, remoteTask2);

        remoteTask2.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                .putAll(new PlanNodeId("sourceId"), splits)
                .build());

        assertEquals(nodeTaskMap.getNodeTotalMemoryUsageInBytes(chosenNode), 200);
        remoteTask1.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                .putAll(new PlanNodeId("sourceId"), splits)
                .build());
        assertEquals(nodeTaskMap.getNodeTotalMemoryUsageInBytes(chosenNode), 200);
        remoteTask1.abort();
        assertEquals(nodeTaskMap.getNodeTotalMemoryUsageInBytes(chosenNode), 100);
        remoteTask2.abort();
        assertEquals(nodeTaskMap.getNodeTotalMemoryUsageInBytes(chosenNode), 0);
    }

    @Test
    public void testMaxTasksPerStageWithLimit()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap, new ThrowingNodeTtlFetcherManager(), new NoOpQueryManager(), new SimpleTtlNodeSelectorConfig());
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 2);

        Set<Split> splits = new HashSet<>();

        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        SplitPlacementResult splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        Set<InternalNode> internalNodes = splitPlacementResult.getAssignments().keySet();
        assertEquals(internalNodes.size(), 1);

        // adding one more split. Total 2
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        splitPlacementResult = nodeSelector.computeAssignments(splits, getRemoteTableScanTask(splitPlacementResult));
        Set<InternalNode> internalNodesSecondCall = splitPlacementResult.getAssignments().keySet();
        assertEquals(internalNodesSecondCall.size(), 2);
        assertTrue(internalNodesSecondCall.containsAll(internalNodes));

        // adding one more split. Total 3
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        splitPlacementResult = nodeSelector.computeAssignments(splits, getRemoteTableScanTask(splitPlacementResult));
        assertEquals(splitPlacementResult.getAssignments().keySet().size(), 2);
        assertEquals(splitPlacementResult.getAssignments().keySet(), internalNodesSecondCall);
    }

    @Test
    public void testMaxTasksPerStageAddingNewNodes()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        LegacyNetworkTopology networkTopology = new LegacyNetworkTopology();
        // refresh interval is 1 nanosecond
        NodeScheduler nodeScheduler = new NodeScheduler(new NetworkLocationCache(networkTopology), networkTopology, nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap, Duration.valueOf("0s"), new ThrowingNodeTtlFetcherManager(), new NoOpQueryManager(), new SimpleTtlNodeSelectorConfig());
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, CONNECTOR_ID, 2);

        Set<Split> splits = new HashSet<>();
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));

        nodeManager.addNode(CONNECTOR_ID, ImmutableList.of(new InternalNode("node1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false)));
        SplitPlacementResult splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        Set<InternalNode> internalNodes = splitPlacementResult.getAssignments().keySet();
        assertEquals(internalNodes.size(), 1);

        nodeManager.addNode(CONNECTOR_ID, ImmutableList.of(new InternalNode("node2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false)));

        splitPlacementResult = nodeSelector.computeAssignments(splits, getRemoteTableScanTask(splitPlacementResult));
        Set<InternalNode> internalNodesSecondCall = splitPlacementResult.getAssignments().keySet();
        assertEquals(internalNodesSecondCall.size(), 2);
        assertTrue(internalNodesSecondCall.containsAll(internalNodes));

        nodeManager.addNode(CONNECTOR_ID, ImmutableList.of(new InternalNode("node2", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false)));
        internalNodes = splitPlacementResult.getAssignments().keySet();
        assertEquals(internalNodes.size(), 2);
        assertTrue(internalNodesSecondCall.containsAll(internalNodes));
    }

    private List<RemoteTask> getRemoteTableScanTask(SplitPlacementResult splitPlacementResult)
    {
        Map<InternalNode, RemoteTask> taskMap = new HashMap<>();
        Multimap<InternalNode, Split> assignments = splitPlacementResult.getAssignments();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        int task = 0;
        for (InternalNode node : assignments.keySet()) {
            TaskId taskId = new TaskId("test", 1, 1, task, 0);
            task++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, ImmutableList.copyOf(assignments.get(node)), nodeTaskMap.createTaskStatsTracker(node, taskId));
            remoteTask.startSplits(25);
            nodeTaskMap.addTask(node, remoteTask);
            taskMap.put(node, remoteTask);
        }
        return ImmutableList.copyOf(taskMap.values());
    }

    private static Session sessionWithMaxUnacknowledgedSplitsPerTask(int maxUnacknowledgedSplitsPerTask)
    {
        return TestingSession.testSessionBuilder()
                .setSystemProperty(MAX_UNACKNOWLEDGED_SPLITS_PER_TASK, Integer.toString(maxUnacknowledgedSplitsPerTask))
                .build();
    }

    private static Session sessionWithTtlAwareSchedulingStrategyAndEstimatedExecutionTime(Duration estimatedExecutionTime)
    {
        return TestingSession.testSessionBuilder()
                .setSystemProperty(RESOURCE_AWARE_SCHEDULING_STRATEGY, NodeSchedulerConfig.ResourceAwareSchedulingStrategy.TTL.name())
                .setResourceEstimates(new Session.ResourceEstimateBuilder().setExecutionTime(estimatedExecutionTime).build())
                .build();
    }

    private static Session sessionWithScheduleSplitsBasedOnTaskLoad(boolean scheduleSplitsBasedOnTaskLoad)
    {
        return TestingSession.testSessionBuilder()
                .setSystemProperty("schedule_splits_based_on_task_load", String.valueOf(scheduleSplitsBasedOnTaskLoad))
                .build();
    }

    @Test
    public void testScheduleSplitsBasedOnTaskLoad()
    {
        List<RemoteTask> existingTasks = new ArrayList<>();
        try {
            // Test with scheduleSplitsBasedOnTaskLoad enabled
            Session taskLoadSession = sessionWithScheduleSplitsBasedOnTaskLoad(true);
            NodeSelector taskLoadNodeSelector = nodeScheduler.createNodeSelector(taskLoadSession, CONNECTOR_ID);

            TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
            MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);

            // Create existing tasks with different split weights to test task load selection.
            // We will have two queries 'test1' and 'test2'.
            // 'test1' would have more load on node 1 and less on node 2 and 3.
            // 'test2' would have more load on nodes 2 and 3 and very little on node 1.
            // Thus, we will have more total load on nodes 2 and 3, but less for 'test1' on them.
            Set<InternalNode> nodes = nodeManager.getActiveConnectorNodes(CONNECTOR_ID);
            Map<InternalNode, RemoteTask> nodeToTaskMap = new HashMap<>();
            for (InternalNode node : nodes) {
                int nodeIndex = Integer.parseInt(node.getNodeIdentifier().substring("other".length()));

                // Create tasks for query 'test1' with different loads: task 1 (for node 1) has more load.
                int initialSplitsCount = (nodeIndex == 1) ? 5 : (nodeIndex == 2) ? 3 : 2; // First task more loaded
                List<Split> initialSplits = new ArrayList<>();
                for (int j = 0; j < initialSplitsCount; j++) {
                    initialSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
                }

                TaskId taskId = new TaskId("test1", 1, 0, nodeIndex, 0);
                MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(
                        taskId, node, initialSplits, nodeTaskMap.createTaskStatsTracker(node, taskId));
                remoteTask.startSplits(1);

                nodeTaskMap.addTask(node, remoteTask);
                nodeToTaskMap.put(node, remoteTask);
                existingTasks.add(remoteTask);

                // Create tasks for query 'test2' with different loads: tasks 2 and 3 (for nodes 2 and 3) have more load.
                initialSplitsCount = (nodeIndex == 1) ? 1 : 7; // First task less loaded
                initialSplits = new ArrayList<>();
                for (int j = 0; j < initialSplitsCount; j++) {
                    initialSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
                }

                taskId = new TaskId("test2", 1, 0, nodeIndex, 0);
                remoteTask = remoteTaskFactory.createTableScanTask(
                        taskId, node, initialSplits, nodeTaskMap.createTaskStatsTracker(node, taskId));
                remoteTask.startSplits(1);

                nodeTaskMap.addTask(node, remoteTask);
            }

            // Split situation is now the following (initial + second query + assigned):
            // other1: 5 + 1 = 6
            // other2: 3 + 7 = 10
            // other3: 2 + 7 = 9
            // The task-based assignment would pick nodes where the tasks of the 1st query have fewer splits,
            // namely nodes 2 and 3, even though they have more splits in total.

            // Create new splits to assign
            Set<Split> newSplits = new HashSet<>();
            int numNewSplits = 4;
            for (int i = 0; i < numNewSplits; i++) {
                newSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
            }

            // Verify that splits were assigned only to nodes 2 and 3
            SplitPlacementResult result = taskLoadNodeSelector.computeAssignments(newSplits, existingTasks);
            Multimap<InternalNode, Split> assignments = result.getAssignments();
            assertEquals(assignments.size(), numNewSplits);
            for (InternalNode node : assignments.keySet()) {
                assertTrue(node.getNodeIdentifier().equals("other2") || node.getNodeIdentifier().equals("other3"));
            }

            Map<RemoteTask, Multimap<PlanNodeId, Split>> splitsForTasks = new HashMap<>();
            PlanNodeId planNodeId = new PlanNodeId("sourceId");
            for (InternalNode node : assignments.keySet()) {
                Multimap<PlanNodeId, Split> splits = ArrayListMultimap.create();
                for (Split split : assignments.get(node)) {
                    splits.put(planNodeId, split);
                }
                nodeToTaskMap.get(node).addSplits(splits);
            }
            // Split situation is now the following (initial + second query + assigned) = task/node:
            // other1: 5 + 1 + 0 = 5/6
            // other2: 3 + 7 + 2 = 5/12
            // other3: 2 + 7 + 2 = 4/12
            // The task-based assignment would pick nodes where the tasks of the 1st query have fewer splits,
            // this time all nodes would be included as the low loaded ones catch up with the high loaded.

            // Verify that splits were assigned to all nodes.
            result = taskLoadNodeSelector.computeAssignments(newSplits, existingTasks);
            assignments = result.getAssignments();
            assertEquals(assignments.size(), numNewSplits);
            for (InternalNode node : assignments.keySet()) {
                assertTrue(node.getNodeIdentifier().equals("other1") || node.getNodeIdentifier().equals("other2") || node.getNodeIdentifier().equals("other3"));
            }
        }
        finally {
            // Cleanup
            for (RemoteTask task : existingTasks) {
                task.abort();
            }
        }
    }

    private static PartitionedSplitsInfo standardWeightSplitsInfo(int splitCount)
    {
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(splitCount, SplitWeight.rawValueForStandardSplitCount(splitCount));
    }

    private static class TestSplitLocal
            implements ConnectorSplit
    {
        private final SplitWeight splitWeight;

        public TestSplitLocal()
        {
            this(SplitWeight.standard());
        }

        public TestSplitLocal(SplitWeight splitWeight)
        {
            this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return HARD_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return ImmutableList.of(HostAddress.fromString("127.0.0.1:11"));
        }

        @Override
        public Object getInfo()
        {
            return this;
        }

        @Override
        public SplitWeight getSplitWeight()
        {
            return splitWeight;
        }
    }

    private static class TestSplitRemote
            implements ConnectorSplit
    {
        private final List<HostAddress> hosts;
        private final SplitWeight splitWeight;

        public TestSplitRemote()
        {
            this(SplitWeight.standard());
        }

        public TestSplitRemote(SplitWeight splitWeight)
        {
            this(HostAddress.fromString("127.0.0.1:" + ThreadLocalRandom.current().nextInt(5000)), splitWeight);
        }

        public TestSplitRemote(HostAddress host)
        {
            this(host, SplitWeight.standard());
        }

        public TestSplitRemote(HostAddress host, SplitWeight splitWeight)
        {
            this.hosts = ImmutableList.of(requireNonNull(host, "host is null"));
            this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NO_PREFERENCE;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return hosts;
        }

        @Override
        public Object getInfo()
        {
            return this;
        }

        @Override
        public SplitWeight getSplitWeight()
        {
            return splitWeight;
        }
    }

    private static class TestAffinitySplitRemote
            extends TestSplitRemote
    {
        private int scheduleIdentifierId;

        public TestAffinitySplitRemote(int scheduleIdentifierId)
        {
            super();
            this.scheduleIdentifierId = scheduleIdentifierId;
        }

        @Override
        public Object getSplitIdentifier()
        {
            return scheduleIdentifierId;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NodeSelectionStrategy.SOFT_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return nodeProvider.get(format("split%d", scheduleIdentifierId));
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            TestAffinitySplitRemote o = (TestAffinitySplitRemote) obj;
            return scheduleIdentifierId == o.scheduleIdentifierId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(scheduleIdentifierId);
        }
    }

    private static class TestHardAffinitySplitRemote
            extends TestSplitRemote
    {
        public TestHardAffinitySplitRemote()
        {
            super();
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return HARD_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return nodeProvider.get(String.valueOf(new Random().nextInt()));
        }
    }

    private static class TestNetworkTopology
            implements NetworkTopology
    {
        @Override
        public NetworkLocation locate(HostAddress address)
        {
            List<String> parts = new ArrayList<>(ImmutableList.copyOf(Splitter.on(".").split(address.getHostText())));
            Collections.reverse(parts);
            return NetworkLocation.create(parts);
        }

        @Override
        public List<String> getLocationSegmentNames()
        {
            return ImmutableList.of("rack", "machine");
        }
    }
}
