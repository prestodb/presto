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

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NetworkLocation;
import com.facebook.presto.execution.scheduler.NetworkLocationCache;
import com.facebook.presto.execution.scheduler.NetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.SplitPlacementResult;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.util.FinalizerService;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
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
    private NodeSelector nodeSelector;
    private Map<InternalNode, RemoteTask> taskMap;
    private ExecutorService remoteTaskExecutor;
    private ScheduledExecutorService remoteTaskScheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        finalizerService = new FinalizerService();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        nodeManager = new InMemoryNodeManager();

        ImmutableList.Builder<InternalNode> nodeBuilder = ImmutableList.builder();
        nodeBuilder.add(new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        ImmutableList<InternalNode> nodes = nodeBuilder.build();
        nodeManager.addNode(CONNECTOR_ID, nodes);
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap);
        // contents of taskMap indicate the node-task map for the current stage
        taskMap = new HashMap<>();
        nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID);
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
    }

    @Test
    public void testScheduleLocal()
    {
        Split split = new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitLocal());
        Set<Split> splits = ImmutableSet.of(split);

        Map.Entry<InternalNode, Split> assignment = Iterables.getOnlyElement(nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments().entries());
        assertEquals(assignment.getKey().getHostAndPort(), split.getPreferredNodes(ImmutableList.of()).get(0));
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
        ImmutableList<InternalNode> nodes = nodeBuilder.build();
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
        NodeScheduler nodeScheduler = new NodeScheduler(locationCache, topology, nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap, new Duration(5, SECONDS));
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID);

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
            TaskId taskId = new TaskId("test", 1, 0, task);
            task++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, ImmutableList.copyOf(assignments.get(node)), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
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
        int rack1 = 0;
        int rack2 = 0;
        for (Split split : unassigned) {
            String rack = topology.locate(split.getPreferredNodes(ImmutableList.of()).get(0)).getSegments().get(0);
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
    public void testAffinityAssignmentNotSupported()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap);
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID, 2);

        Set<Split> splits = new HashSet<>();

        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(HostAddress.fromString("127.0.0.1:10"))));
        splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(HostAddress.fromString("127.0.0.1:10"))));
        SplitPlacementResult splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        Set<InternalNode> internalNodes = splitPlacementResult.getAssignments().keySet();
        // Split doesn't support affinity schedule, fall back to random schedule
        assertEquals(internalNodes.size(), 2);
    }

    @Test
    public void testAffinityAssignment()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap);
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID, 3);

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
    public void testHardAffinityAssignment()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap);
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID, 3);

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
        TaskId taskId1 = new TaskId("test", 1, 0, 1);
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId1));
        nodeTaskMap.addTask(newNode, remoteTask1);

        TaskId taskId2 = new TaskId("test", 1, 0, 2);
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(taskId2, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId2));
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

        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(newNode), 0);
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
            TaskId taskId = new TaskId("test", 1, 0, 1);
            RemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            nodeTaskMap.addTask(node, remoteTask);
            tasks.add(remoteTask);
        }

        TaskId taskId = new TaskId("test", 1, 0, 2);
        RemoteTask newRemoteTask = remoteTaskFactory.createTableScanTask(taskId, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId));
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
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(newNode), 0);
    }

    @Test
    public void testTaskCompletion()
            throws Exception
    {
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        InternalNode chosenNode = Iterables.get(nodeManager.getActiveConnectorNodes(CONNECTOR_ID), 0);
        TaskId taskId = new TaskId("test", 1, 0, 1);
        RemoteTask remoteTask = remoteTaskFactory.createTableScanTask(
                taskId,
                chosenNode,
                ImmutableList.of(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote())),
                nodeTaskMap.createPartitionedSplitCountTracker(chosenNode, taskId));
        nodeTaskMap.addTask(chosenNode, remoteTask);
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 1);
        remoteTask.abort();
        MILLISECONDS.sleep(100); // Sleep until cache expires
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 0);

        remoteTask.abort();
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 0);
    }

    @Test
    public void testSplitCount()
    {
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        InternalNode chosenNode = Iterables.get(nodeManager.getActiveConnectorNodes(CONNECTOR_ID), 0);

        TaskId taskId1 = new TaskId("test", 1, 0, 1);
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1,
                chosenNode,
                ImmutableList.of(
                        new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()),
                        new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote())),
                nodeTaskMap.createPartitionedSplitCountTracker(chosenNode, taskId1));

        TaskId taskId2 = new TaskId("test", 1, 0, 2);
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(
                taskId2,
                chosenNode,
                ImmutableList.of(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote())),
                nodeTaskMap.createPartitionedSplitCountTracker(chosenNode, taskId2));

        nodeTaskMap.addTask(chosenNode, remoteTask1);
        nodeTaskMap.addTask(chosenNode, remoteTask2);
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 3);

        remoteTask1.abort();
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 1);
        remoteTask2.abort();
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 0);
    }

    @Test
    public void testMaxTasksPerStageWittLimit()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap);
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID, 2);

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
        NodeScheduler nodeScheduler = new NodeScheduler(new NetworkLocationCache(networkTopology), networkTopology, nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap, Duration.valueOf("0s"));
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID, 2);

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
            TaskId taskId = new TaskId("test", 1, 1, task);
            task++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, ImmutableList.copyOf(assignments.get(node)), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            remoteTask.startSplits(25);
            nodeTaskMap.addTask(node, remoteTask);
            taskMap.put(node, remoteTask);
        }
        return ImmutableList.copyOf(taskMap.values());
    }

    private static class TestSplitLocal
            implements ConnectorSplit
    {
        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return HARD_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            return ImmutableList.of(HostAddress.fromString("127.0.0.1:11"));
        }

        @Override
        public Object getInfo()
        {
            return this;
        }
    }

    private static class TestSplitRemote
            implements ConnectorSplit
    {
        private final List<HostAddress> hosts;

        public TestSplitRemote()
        {
            this(HostAddress.fromString("127.0.0.1:" + ThreadLocalRandom.current().nextInt(5000)));
        }

        public TestSplitRemote(HostAddress host)
        {
            this.hosts = ImmutableList.of(requireNonNull(host, "host is null"));
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NO_PREFERENCE;
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            return hosts;
        }

        @Override
        public Object getInfo()
        {
            return this;
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
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NodeSelectionStrategy.SOFT_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            return ImmutableList.of(sortedCandidates.get(scheduleIdentifierId % sortedCandidates.size()));
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
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            return ImmutableList.of(sortedCandidates.get(new Random().nextInt(sortedCandidates.size())));
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
