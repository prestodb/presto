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
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestNodeScheduler
{
    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;
    private InMemoryNodeManager nodeManager;
    private NodeScheduler.NodeSelector nodeSelector;
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
        nodeBuilder.add(new PrestoNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN));
        nodeBuilder.add(new PrestoNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN));
        nodeBuilder.add(new PrestoNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN));
        ImmutableList<Node> nodes = nodeBuilder.build();
        nodeManager.addNode("foo", nodes);
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerNodePerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(nodeManager, nodeSchedulerConfig, nodeTaskMap);
        // contents of taskMap indicate the node-task map for the current stage
        taskMap = new HashMap<>();
        nodeSelector = nodeScheduler.createNodeSelector("foo");
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
        Split split = new Split("foo", new TestSplitLocal());
        Set<Split> splits = ImmutableSet.of(split);

        Map.Entry<Node, Split> assignment = Iterables.getOnlyElement(nodeSelector.computeAssignments(splits, taskMap.values()).entries());
        assertEquals(assignment.getKey().getHostAndPort(), split.getAddresses().get(0));
        assertEquals(assignment.getValue(), split);
    }

    @Test
    public void testMultipleTasksPerNode()
    {
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerNodePerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(nodeManager, nodeSchedulerConfig, nodeTaskMap);
        NodeScheduler.NodeSelector nodeSelector = nodeScheduler.createNodeSelector("foo");
        List<Node> nodes = nodeSelector.selectRandomNodes(10);
        assertEquals(nodes.size(), 3);

        nodeSchedulerConfig.setMultipleTasksPerNodeEnabled(true);
        nodeScheduler = new NodeScheduler(nodeManager, nodeSchedulerConfig, nodeTaskMap);
        nodeSelector = nodeScheduler.createNodeSelector("foo");
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
        splits.add(new Split("foo", new TestSplitRemote()));
        Multimap<Node, Split> assignments = nodeSelector.computeAssignments(splits, taskMap.values());
        assertEquals(assignments.size(), 1);
    }

    @Test
    public void testBasicAssignment()
            throws Exception
    {
        // One split for each node
        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            splits.add(new Split("foo", new TestSplitRemote()));
        }
        Multimap<Node, Split> assignments = nodeSelector.computeAssignments(splits, taskMap.values());
        assertEquals(assignments.entries().size(), 3);
        for (Node node : nodeManager.getActiveDatasourceNodes("foo")) {
            assertTrue(assignments.keySet().contains(node));
        }
    }

    @Test
    public void testMaxSplitsPerNode()
            throws Exception
    {
        Node newNode = new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN);
        nodeManager.addNode("foo", newNode);

        ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            initialSplits.add(new Split("foo", new TestSplitRemote()));
        }

        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        // Max out number of splits on node
        TaskId taskId1 = new TaskId(new StageId("test", "1"), "1");
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId1));
        nodeTaskMap.addTask(newNode, remoteTask1);

        TaskId taskId2 = new TaskId(new StageId("test", "1"), "2");
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(taskId2, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId2));
        nodeTaskMap.addTask(newNode, remoteTask2);

        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            splits.add(new Split("foo", new TestSplitRemote()));
        }
        Multimap<Node, Split> assignments = nodeSelector.computeAssignments(splits, taskMap.values());

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
        Node newNode = new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN);
        nodeManager.addNode("foo", newNode);

        ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
        for (int i = 0; i < 20; i++) {
            initialSplits.add(new Split("foo", new TestSplitRemote()));
        }

        List<RemoteTask> tasks = new ArrayList<>();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        for (Node node : nodeManager.getActiveDatasourceNodes("foo")) {
            // Max out number of splits on node
            TaskId taskId = new TaskId(new StageId("test", "1"), "1");
            RemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            nodeTaskMap.addTask(node, remoteTask);
            tasks.add(remoteTask);
        }

        TaskId taskId = new TaskId(new StageId("test", "1"), "2");
        RemoteTask newRemoteTask = remoteTaskFactory.createTableScanTask(taskId, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId));
        // Max out pending splits on new node
        taskMap.put(newNode, newRemoteTask);
        nodeTaskMap.addTask(newNode, newRemoteTask);
        tasks.add(newRemoteTask);

        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            splits.add(new Split("foo", new TestSplitRemote()));
        }
        Multimap<Node, Split> assignments = nodeSelector.computeAssignments(splits, taskMap.values());

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
        Node chosenNode = Iterables.get(nodeManager.getActiveDatasourceNodes("foo"), 0);
        TaskId taskId = new TaskId(new StageId("test", "1"), "1");
        RemoteTask remoteTask = remoteTaskFactory.createTableScanTask(
                taskId,
                chosenNode,
                ImmutableList.of(new Split("foo", new TestSplitRemote())),
                nodeTaskMap.createPartitionedSplitCountTracker(chosenNode, taskId));
        nodeTaskMap.addTask(chosenNode, remoteTask);
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 1);
        remoteTask.abort();
        TimeUnit.MILLISECONDS.sleep(100); // Sleep until cache expires
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 0);

        remoteTask.abort();
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 0);
    }

    @Test
    public void testSplitCount()
            throws Exception
    {
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        Node chosenNode = Iterables.get(nodeManager.getActiveDatasourceNodes("foo"), 0);

        TaskId taskId1 = new TaskId(new StageId("test", "1"), "1");
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1,
                chosenNode,
                ImmutableList.of(new Split("foo", new TestSplitRemote()), new Split("bar", new TestSplitRemote())),
                nodeTaskMap.createPartitionedSplitCountTracker(chosenNode, taskId1));

        TaskId taskId2 = new TaskId(new StageId("test", "1"), "2");
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(
                taskId2,
                chosenNode,
                ImmutableList.of(new Split("foo2", new TestSplitRemote())),
                nodeTaskMap.createPartitionedSplitCountTracker(chosenNode, taskId2));

        nodeTaskMap.addTask(chosenNode, remoteTask1);
        nodeTaskMap.addTask(chosenNode, remoteTask2);
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 3);

        remoteTask1.abort();
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 1);
        remoteTask2.abort();
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode), 0);
    }

    private class TestSplitLocal
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

    private class TestSplitRemote
            implements ConnectorSplit
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            return true;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            int randomPort = ThreadLocalRandom.current().nextInt(5000);
            return ImmutableList.of(HostAddress.fromString("127.0.0.1:" + randomPort));
        }

        @Override
        public Object getInfo()
        {
            return this;
        }
    }
}
