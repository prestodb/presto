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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NetworkLocation;
import com.facebook.presto.execution.scheduler.NetworkLocationCache;
import com.facebook.presto.execution.scheduler.NetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.NodeSelector;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.util.FinalizerService;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
    private Map<Node, RemoteTask> taskMap;
    private ExecutorService remoteTaskExecutor;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        finalizerService = new FinalizerService();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        nodeManager = new InMemoryNodeManager();

        ImmutableList.Builder<Node> nodeBuilder = ImmutableList.builder();
        nodeBuilder.add(new PrestoNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new PrestoNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new PrestoNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        ImmutableList<Node> nodes = nodeBuilder.build();
        nodeManager.addNode(CONNECTOR_ID, nodes);
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerNodePerStage(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, nodeSchedulerConfig, nodeTaskMap);
        // contents of taskMap indicate the node-task map for the current stage
        taskMap = new HashMap<>();
        nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID);
        remoteTaskExecutor = Executors.newCachedThreadPool(daemonThreadsNamed("remoteTaskExecutor-%s"));

        finalizerService.start();
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        remoteTaskExecutor.shutdown();
        finalizerService.destroy();
    }

    @Test
    public void testScheduleLocal()
            throws Exception
    {
        Split split = new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitLocal());
        Set<Split> splits = ImmutableSet.of(split);

        Map.Entry<Node, Split> assignment = Iterables.getOnlyElement(nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).entries());
        assertEquals(assignment.getKey().getHostAndPort(), split.getAddresses().get(0));
        assertEquals(assignment.getValue(), split);
    }

    @Test(timeOut = 60 * 1000)
    public void testTopologyAwareScheduling()
            throws Exception
    {
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        ImmutableList.Builder<Node> nodeBuilder = ImmutableList.builder();
        nodeBuilder.add(new PrestoNode("node1", URI.create("http://host1.rack1:11"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new PrestoNode("node2", URI.create("http://host2.rack1:12"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new PrestoNode("node3", URI.create("http://host3.rack2:13"), NodeVersion.UNKNOWN, false));
        ImmutableList<Node> nodes = nodeBuilder.build();
        nodeManager.addNode(CONNECTOR_ID, nodes);

        // contents of taskMap indicate the node-task map for the current stage
        Map<Node, RemoteTask> taskMap = new HashMap<>();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setNetworkTopology("test")
                .setMaxPendingSplitsPerNodePerStage(15);

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
        NodeScheduler nodeScheduler = new NodeScheduler(locationCache, topology, nodeManager, nodeSchedulerConfig, nodeTaskMap);
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID);

        // Fill up the nodes with non-local data
        ImmutableSet.Builder<Split> nonRackLocalBuilder = ImmutableSet.builder();
        for (int i = 0; i < 26 * 3; i++) {
            nonRackLocalBuilder.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote(HostAddress.fromParts("data.other_rack", 1))));
        }
        Set<Split> nonRackLocalSplits = nonRackLocalBuilder.build();
        Multimap<Node, Split> assignments = nodeSelector.computeAssignments(nonRackLocalSplits, ImmutableList.copyOf(taskMap.values()));
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        int task = 0;
        for (Node node : assignments.keySet()) {
            TaskId taskId = new TaskId("test", 1, task);
            task++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, ImmutableList.copyOf(assignments.get(node)), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            remoteTask.startSplits(20);
            nodeTaskMap.addTask(node, remoteTask);
            taskMap.put(node, remoteTask);
        }
        // Continue assigning to fill up part of the queue
        nonRackLocalSplits = Sets.difference(nonRackLocalSplits, new HashSet<>(assignments.values()));
        assignments = nodeSelector.computeAssignments(nonRackLocalSplits, ImmutableList.copyOf(taskMap.values()));
        for (Node node : assignments.keySet()) {
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
        assignments = nodeSelector.computeAssignments(rackLocalSplits.build(), ImmutableList.copyOf(taskMap.values()));
        for (Node node : assignments.keySet()) {
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
        assignments = nodeSelector.computeAssignments(unassigned, ImmutableList.copyOf(taskMap.values()));
        for (Node node : assignments.keySet()) {
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
            String rack = topology.locate(split.getAddresses().get(0)).getSegments().get(0);
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
        assignments = nodeSelector.computeAssignments(localSplits.build(), ImmutableList.copyOf(taskMap.values()));
        assertEquals(assignments.size(), 3);
        assertEquals(assignments.keySet().size(), 3);
    }

    @Test
    public void testMultipleTasksPerNode()
    {
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerNodePerStage(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, nodeSchedulerConfig, nodeTaskMap);
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID);
        List<Node> nodes = nodeSelector.selectRandomNodes(10);
        assertEquals(nodes.size(), 3);

        nodeSchedulerConfig.setMultipleTasksPerNodeEnabled(true);
        nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, nodeSchedulerConfig, nodeTaskMap);
        nodeSelector = nodeScheduler.createNodeSelector(CONNECTOR_ID);
        nodes = nodeSelector.selectRandomNodes(9);
        assertEquals(nodes.size(), 9);
        Map<String, Integer> counts = new HashMap<>();
        for (Node node : nodes) {
            Integer value = counts.get(node.getNodeIdentifier());
            counts.put(node.getNodeIdentifier(), (value == null ? 0 : value) + 1);
        }
        assertEquals(counts.get("other1").intValue(), 3);
        assertEquals(counts.get("other2").intValue(), 3);
        assertEquals(counts.get("other3").intValue(), 3);
    }

    @Test
    public void testScheduleRemote()
            throws Exception
    {
        Set<Split> splits = new HashSet<>();
        splits.add(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()));
        Multimap<Node, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values()));
        assertEquals(assignments.size(), 1);
    }

    @Test
    public void testBasicAssignment()
            throws Exception
    {
        // One split for each node
        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            splits.add(new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()));
        }
        Multimap<Node, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values()));
        assertEquals(assignments.entries().size(), 3);
        for (Node node : nodeManager.getActiveConnectorNodes(CONNECTOR_ID)) {
            assertTrue(assignments.keySet().contains(node));
        }
    }

    @Test
    public void testMaxSplitsPerNode()
            throws Exception
    {
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();

        Node newNode = new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, newNode);

        ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            initialSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        }

        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        // Max out number of splits on node
        TaskId taskId1 = new TaskId("test", 1, 1);
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId1));
        nodeTaskMap.addTask(newNode, remoteTask1);

        TaskId taskId2 = new TaskId("test", 1, 2);
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(taskId2, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId2));
        nodeTaskMap.addTask(newNode, remoteTask2);

        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        }
        Multimap<Node, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values()));

        // no split should be assigned to the newNode, as it already has maxNodeSplits assigned to it
        assertFalse(assignments.keySet().contains(newNode));

        remoteTask1.abort();
        remoteTask2.abort();

        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(newNode), 0);
    }

    @Test
    public void testMaxSplitsPerNodePerTask()
            throws Exception
    {
        TestingTransactionHandle transactionHandle = TestingTransactionHandle.create();

        Node newNode = new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, newNode);

        ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
        for (int i = 0; i < 20; i++) {
            initialSplits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        }

        List<RemoteTask> tasks = new ArrayList<>();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        for (Node node : nodeManager.getActiveConnectorNodes(CONNECTOR_ID)) {
            // Max out number of splits on node
            TaskId taskId = new TaskId("test", 1, 1);
            RemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            nodeTaskMap.addTask(node, remoteTask);
            tasks.add(remoteTask);
        }

        TaskId taskId = new TaskId("test", 1, 2);
        RemoteTask newRemoteTask = remoteTaskFactory.createTableScanTask(taskId, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId));
        // Max out pending splits on new node
        taskMap.put(newNode, newRemoteTask);
        nodeTaskMap.addTask(newNode, newRemoteTask);
        tasks.add(newRemoteTask);

        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            splits.add(new Split(CONNECTOR_ID, transactionHandle, new TestSplitRemote()));
        }
        Multimap<Node, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values()));

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
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        Node chosenNode = Iterables.get(nodeManager.getActiveConnectorNodes(CONNECTOR_ID), 0);
        TaskId taskId = new TaskId("test", 1, 1);
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
            throws Exception
    {
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        Node chosenNode = Iterables.get(nodeManager.getActiveConnectorNodes(CONNECTOR_ID), 0);

        TaskId taskId1 = new TaskId("test", 1, 1);
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1,
                chosenNode,
                ImmutableList.of(
                        new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote()),
                        new Split(CONNECTOR_ID, TestingTransactionHandle.create(), new TestSplitRemote())),
                nodeTaskMap.createPartitionedSplitCountTracker(chosenNode, taskId1));

        TaskId taskId2 = new TaskId("test", 1, 2);
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

    private static class TestSplitLocal
            implements ConnectorSplit
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            return false;
        }

        @Override
        public List<HostAddress> getAddresses()
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
        public boolean isRemotelyAccessible()
        {
            return true;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return hosts;
        }

        @Override
        public Object getInfo()
        {
            return this;
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
