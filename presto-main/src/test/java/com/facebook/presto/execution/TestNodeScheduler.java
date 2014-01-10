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

import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplit;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.util.Threads;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestNodeScheduler
{
    public static final Session SESSION = new Session("user", "source", "catalog", "schema", "address", "agent");
    private NodeTaskMap nodeTaskMap;
    private NodeScheduler nodeScheduler;
    private InMemoryNodeManager nodeManager;
    private MetadataManager metadata;

    private final String datasource = "test_datasource";

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        ImmutableList.Builder<Node> nodeBuilder = ImmutableList.builder();
        nodeBuilder.add(new PrestoNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN));
        nodeBuilder.add(new PrestoNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN));
        nodeBuilder.add(new PrestoNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN));
        ImmutableList<Node> nodes = nodeBuilder.build();
        this.nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(datasource, nodes);

        this.nodeTaskMap = new NodeTaskMap();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig().setMaxSplitsPerNode(30);
        this.nodeScheduler = new NodeScheduler(nodeManager, nodeSchedulerConfig, nodeTaskMap);

        metadata = new MetadataManager();
        metadata.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, new DualMetadata());
    }

    @Test
    public void testBasicAssignment()
            throws Exception
    {
        NodeScheduler.NodeSelector nodeSelector = nodeScheduler.createNodeSelector(datasource);
        Multimap<Node, Split> splitAssignment = nodeSelector.computeAssignments(ImmutableSet.copyOf(getSplits(15)));
        assertEquals(splitAssignment.asMap().size(), 3);
        for (Map.Entry<Node, Collection<Split>> entry : splitAssignment.asMap().entrySet()) {
            assertEquals(entry.getValue().size(), 5);
        }
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No nodes available to run query")
    public void testNoAvailableNodes()
            throws Exception
    {
        NodeScheduler.NodeSelector nodeSelector = nodeScheduler.createNodeSelector("foo");
        nodeSelector.computeAssignments(ImmutableSet.copyOf(getSplits(3)));
        fail("expected exception");
    }

    @Test
    public void testGlobalAssignment()
            throws Exception
    {
        ExecutorService remoteTaskExecutor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("remoteTaskExecutor"));
        MockRemoteTaskFactory taskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        Multimap<Node, RemoteTask> nodeToTask = HashMultimap.create();

        // Add new node
        Node additionalNode = new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN);
        nodeManager.addNode(datasource, additionalNode);

        // initialize all nodes with tasks
        for (Node node : nodeManager.getActiveNodes()) {
            String queryId = "query_1";
            StageExecutionPlan tableScanPlan = TestSqlStageExecution.createTableScanPlan("test", datasource, metadata, 0);
            RemoteTask task;
            if (node == additionalNode) {
                task = createRemoteTask(taskFactory, tableScanPlan, queryId, node, 5);
            }
            else {
                task = createRemoteTask(taskFactory, tableScanPlan, queryId, node, 10);
            }
            nodeTaskMap.addTask(node, task);
            nodeToTask.put(node, task);
        }

        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(additionalNode), 5);

        // Check that the new assignment selects the additionalNode
        NodeScheduler.NodeSelector nodeSelector = nodeScheduler.createNodeSelector(datasource);
        // Compute node assignment for a single split
        Multimap<Node, Split> splitAssignment = nodeSelector.computeAssignments(ImmutableSet.copyOf(getSplits(1)));
        assertEquals(splitAssignment.get(additionalNode).size(), 1);
    }

    @Test
    public void testMultipleTasks()
            throws Exception
    {
        ExecutorService remoteTaskExecutor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("remoteTaskExecutor"));
        MockRemoteTaskFactory taskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);

        // Add new node
        Node additionalNode = new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN);
        nodeManager.addNode(datasource, additionalNode);

        StageExecutionPlan tableScanPlan = TestSqlStageExecution.createTableScanPlan("test", datasource, metadata, 0);
        RemoteTask task1 = createRemoteTask(taskFactory, tableScanPlan, "query_1", additionalNode, 5);
        nodeTaskMap.addTask(additionalNode, task1);
        RemoteTask task2 = createRemoteTask(taskFactory, tableScanPlan, "query_2", additionalNode, 10);
        nodeTaskMap.addTask(additionalNode, task2);

        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(additionalNode), 15);
    }

    private Collection<Split> getSplits(int splitCount)
    {
        ImmutableList.Builder<Split> splits = ImmutableList.builder();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new DualSplit(HostAddress.fromString("127.0.0.1")));
        }
        return splits.build();
    }

    private RemoteTask createRemoteTask(RemoteTaskFactory taskFactory, StageExecutionPlan plan, String queryId, Node node, int splitCount)
    {
        TaskId taskId = new TaskId(new StageId(queryId, "1"), "2");
        PlanNodeId id = new PlanNodeId("test");

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        for (int i = 0; i < splitCount; i++) {
            initialSplits.put(id, new DualSplit(HostAddress.fromString("127.0.0.1")));
        }
        return taskFactory.createRemoteTask(SESSION, taskId, node, plan.getFragment(), initialSplits.build(), null);
    }
}
