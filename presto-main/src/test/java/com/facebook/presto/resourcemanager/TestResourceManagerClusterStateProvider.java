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
package com.facebook.presto.resourcemanager;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.memory.MemoryInfo;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.DISPATCHING;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.FINISHING;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.STARTING;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_RESOURCES;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestResourceManagerClusterStateProvider
{
    @Test(timeOut = 15_000)
    public void testQueryInfo()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node2", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));

        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(nodeManager, new SessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("5s"), Duration.valueOf("0s"), true, newSingleThreadScheduledExecutor());

        assertEquals(provider.getClusterQueries(), ImmutableList.of());

        long query1Sequence = 0;
        long query2Sequence = 0;
        long query3Sequence = 0;
        long query4Sequence = 0;
        provider.registerQueryHeartbeat("node1", createQueryInfo("1", QUEUED), query1Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("2", RUNNING), query2Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("3", FINISHED), query3Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("4", FAILED), query4Sequence++);

        assertQueryInfos(provider.getClusterQueries(), 4, 2);

        provider.registerQueryHeartbeat("node1", createQueryInfo("1", RUNNING), query1Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("2", FINISHING), query2Sequence++);

        assertQueryInfos(provider.getClusterQueries(), 4, 2);

        // Update query 2 to FINISHED to verify this is now completed in the resource manager
        provider.registerQueryHeartbeat("node1", createQueryInfo("2", FINISHED), query2Sequence++);

        assertQueryInfos(provider.getClusterQueries(), 4, 3);

        // Mix in queries from another coordinator
        provider.registerQueryHeartbeat("node2", createQueryInfo("1", QUEUED), query1Sequence++);
        provider.registerQueryHeartbeat("node2", createQueryInfo("2", RUNNING), query2Sequence++);
        provider.registerQueryHeartbeat("node2", createQueryInfo("3", FINISHED), query3Sequence++);
        provider.registerQueryHeartbeat("node2", createQueryInfo("4", FAILED), query4Sequence++);

        assertQueryInfos(provider.getClusterQueries(), 8, 5);

        // Expire completed queries
        Thread.sleep(SECONDS.toMillis(5));

        assertQueryInfos(provider.getClusterQueries(), 8, 5);

        // Expire all queries
        Thread.sleep(SECONDS.toMillis(5));

        assertQueryInfos(provider.getClusterQueries(), 0, 0);
    }
    @Test(timeOut = 15_000)
    public void testOutOfOrderUpdatesIgnored()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node2", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));

        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(nodeManager, new SessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("5s"), Duration.valueOf("0s"), true, newSingleThreadScheduledExecutor());

        assertEquals(provider.getClusterQueries(), ImmutableList.of());

        provider.registerQueryHeartbeat("node1", createQueryInfo("1", QUEUED), 1);
        provider.registerQueryHeartbeat("node1", createQueryInfo("2", FINISHED), 2);

        assertQueryInfos(provider.getClusterQueries(), 2, 1);

        provider.registerQueryHeartbeat("node1", createQueryInfo("1", FINISHED), 0);
        provider.registerQueryHeartbeat("node1", createQueryInfo("2", RUNNING), 1);

        assertQueryInfos(provider.getClusterQueries(), 2, 1);

        provider.registerQueryHeartbeat("node1", createQueryInfo("1", FINISHED), 2);

        assertQueryInfos(provider.getClusterQueries(), 2, 2);
    }

    @Test(timeOut = 15_000)
    public void testResourceGroups()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node2", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node3", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));

        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(nodeManager, new SessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("50s"), Duration.valueOf("0s"), true, newSingleThreadScheduledExecutor());
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("local"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node1"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node2"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node3"));

        assertEquals(provider.getClusterQueries(), ImmutableList.of());

        long query1Sequence = 0;
        long query2Sequence = 0;
        long query3Sequence = 0;
        long query4Sequence = 0;
        long query5Sequence = 0;
        long query6Sequence = 0;
        provider.registerQueryHeartbeat("node1", createQueryInfo("1", QUEUED, "rg1", GENERAL_POOL), query1Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("2", RUNNING, "rg2", GENERAL_POOL), query2Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("3", FINISHING, "rg3", GENERAL_POOL), query3Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("4", FINISHED, "rg4", GENERAL_POOL), query4Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("5", FAILED, "rg5", GENERAL_POOL), query5Sequence++);
        assertResourceGroups(provider, "node1", 0);
        assertResourceGroups(provider, "node2", 3);

        // Add an existing leaf node from another node
        provider.registerQueryHeartbeat("node3", createQueryInfo("6", QUEUED, "rg6", GENERAL_POOL), query6Sequence++);
        assertResourceGroups(provider, "node1", 1);
        assertResourceGroups(provider, "node2", 4);
        assertResourceGroups(provider, "node3", 3);

        // Expire running queries
        Thread.sleep(SECONDS.toMillis(5));
        assertResourceGroups(provider, "node1", 0);
        assertResourceGroups(provider, "node2", 0);
        assertResourceGroups(provider, "node3", 0);
    }

    @Test(timeOut = 15_000)
    public void testResourceGroupsMerged()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node2", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node3", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node4", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node5", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node6", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));

        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(nodeManager, new SessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("50s"), Duration.valueOf("0s"), true, newSingleThreadScheduledExecutor());
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("local"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node1"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node2"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node3"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node4"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node5"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node6"));

        assertEquals(provider.getClusterQueries(), ImmutableList.of());

        provider.registerQueryHeartbeat("node1", createQueryInfo("1", QUEUED, "rg4", GENERAL_POOL), 0);
        assertTrue(provider.getClusterResourceGroups("node1").isEmpty());
        assertResourceGroup(provider, "node2", "rg4", 1, 0, DataSize.valueOf("1MB"));

        provider.registerQueryHeartbeat("node2", createQueryInfo("2", RUNNING, "rg4", GENERAL_POOL), 0);
        assertResourceGroup(provider, "node1", "rg4", 0, 1, DataSize.valueOf("1MB"));
        assertResourceGroup(provider, "node2", "rg4", 1, 0, DataSize.valueOf("1MB"));
        assertResourceGroup(provider, "node3", "rg4", 1, 1, DataSize.valueOf("2MB"));

        provider.registerQueryHeartbeat("node3", createQueryInfo("3", FINISHED, "rg4", GENERAL_POOL), 0);
        assertResourceGroup(provider, "node1", "rg4", 0, 1, DataSize.valueOf("1MB"));
        assertResourceGroup(provider, "node2", "rg4", 1, 0, DataSize.valueOf("1MB"));
        assertResourceGroup(provider, "node3", "rg4", 1, 1, DataSize.valueOf("2MB"));
        assertResourceGroup(provider, "node4", "rg4", 1, 1, DataSize.valueOf("2MB"));

        provider.registerQueryHeartbeat("node4", createQueryInfo("4", FAILED, "rg4", GENERAL_POOL), 0);
        assertResourceGroup(provider, "node1", "rg4", 0, 1, DataSize.valueOf("1MB"));
        assertResourceGroup(provider, "node2", "rg4", 1, 0, DataSize.valueOf("1MB"));
        assertResourceGroup(provider, "node3", "rg4", 1, 1, DataSize.valueOf("2MB"));
        assertResourceGroup(provider, "node4", "rg4", 1, 1, DataSize.valueOf("2MB"));
        assertResourceGroup(provider, "node5", "rg4", 1, 1, DataSize.valueOf("2MB"));

        // Add queries which are in non-terminal states other than RUNNING and QUEUED
        provider.registerQueryHeartbeat("node1", createQueryInfo("5", WAITING_FOR_RESOURCES, "rg4", GENERAL_POOL), 0);
        provider.registerQueryHeartbeat("node2", createQueryInfo("6", DISPATCHING, "rg4", GENERAL_POOL), 0);
        provider.registerQueryHeartbeat("node3", createQueryInfo("7", PLANNING, "rg4", GENERAL_POOL), 0);
        provider.registerQueryHeartbeat("node4", createQueryInfo("8", STARTING, "rg4", GENERAL_POOL), 0);
        provider.registerQueryHeartbeat("node5", createQueryInfo("9", FINISHING, "rg4", GENERAL_POOL), 0);
        assertResourceGroup(provider, "node1", "rg4", 0, 5, DataSize.valueOf("5MB"));
        assertResourceGroup(provider, "node2", "rg4", 1, 4, DataSize.valueOf("5MB"));
        assertResourceGroup(provider, "node3", "rg4", 1, 5, DataSize.valueOf("6MB"));
        assertResourceGroup(provider, "node4", "rg4", 1, 5, DataSize.valueOf("6MB"));
        assertResourceGroup(provider, "node5", "rg4", 1, 5, DataSize.valueOf("6MB"));
        assertResourceGroup(provider, "node6", "rg4", 1, 6, DataSize.valueOf("7MB"));

        // Expire running queries
        Thread.sleep(SECONDS.toMillis(5));
        assertTrue(provider.getClusterResourceGroups("node1").isEmpty());
        assertTrue(provider.getClusterResourceGroups("node2").isEmpty());
        assertTrue(provider.getClusterResourceGroups("node3").isEmpty());
        assertTrue(provider.getClusterResourceGroups("node4").isEmpty());
        assertTrue(provider.getClusterResourceGroups("node5").isEmpty());
        assertTrue(provider.getClusterResourceGroups("node6").isEmpty());
    }

    @Test
    public void testNonLeafResourceGroupsMerged()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node2", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node3", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node4", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node5", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node6", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(nodeManager, new SessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("50s"), Duration.valueOf("0s"), true, newSingleThreadScheduledExecutor());
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("local"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node1"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node2"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node3"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node4"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node5"));
        provider.registerNodeHeartbeat(createCoordinatorNodeStatus("node6"));

        long query1Sequence = 0;
        long query2Sequence = 0;
        long query3Sequence = 0;
        long query4Sequence = 0;
        long query5Sequence = 0;
        long query6Sequence = 0;
        long query7Sequence = 0;
        long query8Sequence = 0;
        long query9Sequence = 0;

        assertEquals(provider.getClusterQueries(), ImmutableList.of());

        provider.registerQueryHeartbeat("node1", createQueryInfo("1", QUEUED, "root.rg4", GENERAL_POOL), query1Sequence++);
        assertTrue(provider.getClusterResourceGroups("node1").isEmpty());
        assertResourceGroup(provider, "node2", "root.rg4", 1, 0, DataSize.valueOf("1MB"));
        assertNonLeafResourceGroup(provider, "node2", "root", 0, 0, 1, 0);

        provider.registerQueryHeartbeat("node2", createQueryInfo("2", RUNNING, "root.rg4", GENERAL_POOL), query2Sequence++);
        assertResourceGroup(provider, "node1", "root.rg4", 0, 1, DataSize.valueOf("1MB"));
        assertNonLeafResourceGroup(provider, "node1", "root", 0, 0, 0, 1);
        assertResourceGroup(provider, "node2", "root.rg4", 1, 0, DataSize.valueOf("1MB"));
        assertNonLeafResourceGroup(provider, "node1", "root", 0, 0, 0, 1);
        assertResourceGroup(provider, "node3", "root.rg4", 1, 1, DataSize.valueOf("2MB"));

        provider.registerQueryHeartbeat("node3", createQueryInfo("3", FINISHED, "root.rg4", GENERAL_POOL), query3Sequence++);
        assertResourceGroup(provider, "node1", "root.rg4", 0, 1, DataSize.valueOf("1MB"));
        assertNonLeafResourceGroup(provider, "node1", "root", 0, 0, 0, 1);
        assertResourceGroup(provider, "node2", "root.rg4", 1, 0, DataSize.valueOf("1MB"));
        assertNonLeafResourceGroup(provider, "node2", "root", 0, 0, 1, 0);
        assertResourceGroup(provider, "node3", "root.rg4", 1, 1, DataSize.valueOf("2MB"));
        assertNonLeafResourceGroup(provider, "node3", "root", 0, 0, 1, 1);
        assertResourceGroup(provider, "node4", "root.rg4", 1, 1, DataSize.valueOf("2MB"));
        assertNonLeafResourceGroup(provider, "node4", "root", 0, 0, 1, 1);

        provider.registerQueryHeartbeat("node4", createQueryInfo("4", FAILED, "root.rg4", GENERAL_POOL), query4Sequence++);
        assertResourceGroup(provider, "node1", "root.rg4", 0, 1, DataSize.valueOf("1MB"));
        assertNonLeafResourceGroup(provider, "node1", "root", 0, 0, 0, 1);
        assertResourceGroup(provider, "node2", "root.rg4", 1, 0, DataSize.valueOf("1MB"));
        assertNonLeafResourceGroup(provider, "node2", "root", 0, 0, 1, 0);
        assertResourceGroup(provider, "node3", "root.rg4", 1, 1, DataSize.valueOf("2MB"));
        assertNonLeafResourceGroup(provider, "node3", "root", 0, 0, 1, 1);
        assertResourceGroup(provider, "node4", "root.rg4", 1, 1, DataSize.valueOf("2MB"));
        assertNonLeafResourceGroup(provider, "node4", "root", 0, 0, 1, 1);
        assertResourceGroup(provider, "node5", "root.rg4", 1, 1, DataSize.valueOf("2MB"));
        assertNonLeafResourceGroup(provider, "node5", "root", 0, 0, 1, 1);

        // Add queries which are in non-terminal states other than RUNNING and QUEUED
        provider.registerQueryHeartbeat("node1", createQueryInfo("5", WAITING_FOR_RESOURCES, "root.rg4", GENERAL_POOL), query5Sequence++);
        provider.registerQueryHeartbeat("node2", createQueryInfo("6", DISPATCHING, "root.rg4", GENERAL_POOL), query6Sequence++);
        provider.registerQueryHeartbeat("node3", createQueryInfo("7", PLANNING, "root.rg4", GENERAL_POOL), query7Sequence++);
        provider.registerQueryHeartbeat("node4", createQueryInfo("8", STARTING, "root.rg4", GENERAL_POOL), query8Sequence++);
        provider.registerQueryHeartbeat("node5", createQueryInfo("9", FINISHING, "root.rg4", GENERAL_POOL), query9Sequence++);
        assertResourceGroup(provider, "node1", "root.rg4", 0, 5, DataSize.valueOf("5MB"));
        assertNonLeafResourceGroup(provider, "node1", "root", 0, 0, 0, 5);
        assertResourceGroup(provider, "node2", "root.rg4", 1, 4, DataSize.valueOf("5MB"));
        assertNonLeafResourceGroup(provider, "node2", "root", 0, 0, 1, 4);
        assertResourceGroup(provider, "node3", "root.rg4", 1, 5, DataSize.valueOf("6MB"));
        assertNonLeafResourceGroup(provider, "node3", "root", 0, 0, 1, 5);
        assertResourceGroup(provider, "node4", "root.rg4", 1, 5, DataSize.valueOf("6MB"));
        assertNonLeafResourceGroup(provider, "node4", "root", 0, 0, 1, 5);
        assertResourceGroup(provider, "node5", "root.rg4", 1, 5, DataSize.valueOf("6MB"));
        assertNonLeafResourceGroup(provider, "node5", "root", 0, 0, 1, 5);
        assertResourceGroup(provider, "node6", "root.rg4", 1, 6, DataSize.valueOf("7MB"));
        assertNonLeafResourceGroup(provider, "node6", "root", 0, 0, 1, 6);

        // Expire running queries
        Thread.sleep(SECONDS.toMillis(5));
        nodeManager.refreshNodes();
        assertTrue(provider.getClusterResourceGroups("node1").isEmpty());
        assertTrue(provider.getClusterResourceGroups("node2").isEmpty());
        assertTrue(provider.getClusterResourceGroups("node3").isEmpty());
        assertTrue(provider.getClusterResourceGroups("node4").isEmpty());
        assertTrue(provider.getClusterResourceGroups("node5").isEmpty());
        assertTrue(provider.getClusterResourceGroups("node6").isEmpty());
    }

    @Test(timeOut = 15_000)
    public void testClusterMemoryPoolInfo()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("nodeId1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("nodeId2", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("nodeId3", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("nodeId4", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));

        long query1Sequence = 0;
        long query2Sequence = 0;
        long query3Sequence = 0;

        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(nodeManager, new SessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("4s"), Duration.valueOf("0s"), true, newSingleThreadScheduledExecutor());

        // Memory pool starts off empty
        assertMemoryPoolMap(provider, 2, GENERAL_POOL, 0, 0, 0, 0, 0, Optional.empty());
        assertMemoryPoolMap(provider, 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());

        // Create a node and heartbeat to the resource manager
        provider.registerNodeHeartbeat(createNodeStatus("nodeId", GENERAL_POOL, createMemoryPoolInfo(100, 2, 1)));
        assertMemoryPoolMap(provider, 2, GENERAL_POOL, 0, 0, 100, 2, 1, Optional.empty());
        assertMemoryPoolMap(provider, 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());

        // Register a query and heartbeat that to the resource manager
        provider.registerQueryHeartbeat("nodeId1", createQueryInfo("1", QUEUED, "rg4", GENERAL_POOL), query1Sequence++);
        assertMemoryPoolMap(provider, 2, GENERAL_POOL, 1, 0, 100, 2, 1, Optional.of("1"));
        assertMemoryPoolMap(provider, 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());

        // Create another node and heartbeat to the resource manager
        provider.registerNodeHeartbeat(createNodeStatus("nodeId2", GENERAL_POOL, createMemoryPoolInfo(1000, 20, 10)));
        assertMemoryPoolMap(provider, 2, GENERAL_POOL, 1, 0, 1100, 22, 11, Optional.of("1"));
        assertMemoryPoolMap(provider, 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());

        // Create a blocked node and heartbeat to the resource manager
        provider.registerNodeHeartbeat(createNodeStatus("nodeId3", GENERAL_POOL, createMemoryPoolInfo(1, 2, 3)));
        assertMemoryPoolMap(provider, 2, GENERAL_POOL, 1, 1, 1101, 24, 14, Optional.of("1"));
        assertMemoryPoolMap(provider, 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());

        // Create a node that has only reserved pool allocations
        provider.registerNodeHeartbeat(createNodeStatus("nodeId4", RESERVED_POOL, createMemoryPoolInfo(5, 3, 2)));
        assertMemoryPoolMap(provider, 2, GENERAL_POOL, 1, 1, 1101, 24, 14, Optional.of("1"));
        assertMemoryPoolMap(provider, 2, RESERVED_POOL, 0, 0, 5, 3, 2, Optional.empty());

        // Add a larger query and verify that the largest query is updated
        provider.registerQueryHeartbeat("nodeId2", createQueryInfo("2", RUNNING, "rg4", GENERAL_POOL, DataSize.valueOf("25GB")), query2Sequence++);
        assertMemoryPoolMap(provider, 2, GENERAL_POOL, 2, 1, 1101, 24, 14, Optional.of("2"));
        assertMemoryPoolMap(provider, 2, RESERVED_POOL, 0, 0, 5, 3, 2, Optional.empty());

        // Adding a larger reserved pool query does not affect largest query in general pool
        provider.registerQueryHeartbeat("nodeId1", createQueryInfo("3", RUNNING, "rg4", RESERVED_POOL, DataSize.valueOf("50GB")), query3Sequence++);
        assertMemoryPoolMap(provider, 2, GENERAL_POOL, 2, 1, 1101, 24, 14, Optional.of("2"));
        assertMemoryPoolMap(provider, 2, RESERVED_POOL, 1, 0, 5, 3, 2, Optional.empty());

        // Expire nodes
        Thread.sleep(SECONDS.toMillis(5));

        // All nodes expired, memory pools emptied
        assertMemoryPoolMap(provider, 2, GENERAL_POOL, 0, 0, 0, 0, 0, Optional.empty());
        assertMemoryPoolMap(provider, 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());
    }

    @Test(timeOut = 15_000)
    public void testWorkerMemoryInfo()
            throws Exception
    {
        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(new InMemoryNodeManager(), new SessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("4s"), Duration.valueOf("0s"), true, newSingleThreadScheduledExecutor());

        assertWorkerMemoryInfo(provider, 0);

        provider.registerNodeHeartbeat(createNodeStatus("nodeId", GENERAL_POOL, createMemoryPoolInfo(100, 2, 1)));
        assertWorkerMemoryInfo(provider, 1);

        provider.registerNodeHeartbeat(createNodeStatus("nodeId2", GENERAL_POOL, createMemoryPoolInfo(200, 20, 10)));
        assertWorkerMemoryInfo(provider, 2);

        // Expire nodes
        Thread.sleep(SECONDS.toMillis(5));

        assertWorkerMemoryInfo(provider, 0);
    }

    @Test(timeOut = 15_000)
    public void testShuttingDownCoordinatorHeartbeat()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addShuttingDownNode(new InternalNode("node1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));

        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(nodeManager, new SessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("5s"), Duration.valueOf("0s"), true, newSingleThreadScheduledExecutor());

        assertEquals(provider.getClusterQueries(), ImmutableList.of());

        long query1Sequence = 0;
        long query2Sequence = 0;
        long query3Sequence = 0;
        long query4Sequence = 0;

        provider.registerQueryHeartbeat("node1", createQueryInfo("1", QUEUED), query1Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("2", RUNNING), query2Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("3", FINISHED), query3Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("4", FAILED), query4Sequence++);

        assertQueryInfos(provider.getClusterQueries(), 4, 2);

        provider.registerQueryHeartbeat("node1", createQueryInfo("1", RUNNING), query1Sequence++);
        provider.registerQueryHeartbeat("node1", createQueryInfo("2", FINISHING), query2Sequence++);

        assertQueryInfos(provider.getClusterQueries(), 4, 2);

        provider.registerQueryHeartbeat("node1", createQueryInfo("2", FINISHED), query2Sequence++);

        assertQueryInfos(provider.getClusterQueries(), 4, 3);
    }

    void assertWorkerMemoryInfo(ResourceManagerClusterStateProvider provider, int count)
    {
        Map<String, MemoryInfo> workerMemoryInfo = provider.getWorkerMemoryInfo();
        assertNotNull(workerMemoryInfo);
        assertEquals(workerMemoryInfo.size(), count);
    }

    private NodeStatus createNodeStatus(String nodeId, MemoryPoolId memoryPoolId, MemoryPoolInfo memoryPoolInfo)
    {
        return new NodeStatus(
                nodeId,
                new NodeVersion("1"),
                "environment",
                false,
                new Duration(1, SECONDS),
                "http://exernalAddress",
                "http://internalAddress",
                new MemoryInfo(new DataSize(1, MEGABYTE), ImmutableMap.of(memoryPoolId, memoryPoolInfo)),
                1,
                1.0,
                2.0,
                1,
                2,
                3);
    }

    private NodeStatus createCoordinatorNodeStatus(String nodeId)
    {
        return new NodeStatus(
                nodeId,
                new NodeVersion("1"),
                "environment",
                true,
                new Duration(1, SECONDS),
                "http://exernalAddress",
                "http://internalAddress",
                new MemoryInfo(new DataSize(1, MEGABYTE), ImmutableMap.of(GENERAL_POOL, createMemoryPoolInfo(100, 2, 1))),
                1,
                1.0,
                2.0,
                1,
                2,
                3);
    }

    private MemoryPoolInfo createMemoryPoolInfo(int maxBytes, int reservedBytes, int reservedRevocableBytes)
    {
        return new MemoryPoolInfo(
                maxBytes,
                reservedBytes,
                reservedRevocableBytes,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    private void assertQueryInfos(List<BasicQueryInfo> queryInfos, int count, int numberDone)
    {
        assertNotNull(queryInfos);
        assertEquals(queryInfos.size(), count);
        assertEquals(queryInfos.stream().filter(info -> info.getState().isDone()).count(), numberDone);
    }

    private void assertResourceGroups(ResourceManagerClusterStateProvider provider, String excludingNode, int count)
            throws ResourceManagerInconsistentException
    {
        List<ResourceGroupRuntimeInfo> resourceGroups = provider.getClusterResourceGroups(excludingNode);
        assertNotNull(resourceGroups);
        assertEquals(resourceGroups.size(), count);
    }

    private void assertResourceGroup(ResourceManagerClusterStateProvider provider, String excludingNode, String resourceGroupId, int queuedQueries, int runningQueries, DataSize userMemoryReservation)
            throws ResourceManagerInconsistentException
    {
        ResourceGroupId currResourceGroupId = new ResourceGroupId(Arrays.asList(resourceGroupId.split("\\.")));
        List<ResourceGroupRuntimeInfo> list = provider.getClusterResourceGroups(excludingNode);
        Optional<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo = list.stream()
                .filter(resourceGroupInfo -> currResourceGroupId.equals(resourceGroupInfo.getResourceGroupId()))
                .findFirst();
        assertTrue(resourceGroupRuntimeInfo.isPresent(), "Resource group " + resourceGroupId + " not found");
        ResourceGroupRuntimeInfo info = resourceGroupRuntimeInfo.get();
        ResourceGroupId rg = new ResourceGroupId(Arrays.asList(resourceGroupId.split("\\.")));
        assertEquals(info.getQueuedQueries(), queuedQueries, format("Expected %s queued queries, found %s", queuedQueries, info.getQueuedQueries()));
        assertEquals(info.getRunningQueries(), runningQueries, format("Expected %s running queries, found %s", runningQueries, info.getRunningQueries()));
        assertEquals(info.getResourceGroupId(), rg, format("Expected resource group id %s, found %s", resourceGroupId, info.getResourceGroupId()));
        assertEquals(info.getMemoryUsageBytes(), userMemoryReservation.toBytes(), format("Expected %s user memory reservation found %s", userMemoryReservation, DataSize.succinctBytes(info.getMemoryUsageBytes())));
    }

    private void assertNonLeafResourceGroup(ResourceManagerClusterStateProvider provider, String excludingNode, String resourceGroupId, int queuedQueries, int runningQueries, int descendantQueuedQueries, int descendantRunningQueries)
            throws ResourceManagerInconsistentException
    {
        List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos = provider.getClusterResourceGroups(excludingNode);
        Optional<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo = provider.getClusterResourceGroups(excludingNode).stream()
                .filter(resourceGroupInfo -> new ResourceGroupId(resourceGroupId).equals(resourceGroupInfo.getResourceGroupId()))
                .findFirst();
        assertTrue(resourceGroupRuntimeInfo.isPresent(), "Resource group " + resourceGroupId + " not found");
        ResourceGroupRuntimeInfo info = resourceGroupRuntimeInfo.get();

        assertEquals(info.getQueuedQueries(), queuedQueries, format("Expected %s queued queries, found %s", queuedQueries, info.getQueuedQueries()));
        assertEquals(info.getRunningQueries(), runningQueries, format("Expected %s running queries, found %s", runningQueries, info.getRunningQueries()));
        assertEquals(info.getDescendantQueuedQueries(), descendantQueuedQueries, format("Expected %s descendant queued queries, found %s", descendantQueuedQueries, info.getDescendantQueuedQueries()));
        assertEquals(info.getDescendantRunningQueries(), descendantRunningQueries, format("Expected %s descendant running queries, found %s", descendantRunningQueries, info.getDescendantRunningQueries()));
        assertEquals(info.getResourceGroupId(), new ResourceGroupId(resourceGroupId), format("Expected resource group id %s, found %s", resourceGroupId, info.getResourceGroupId()));
    }

    private void assertMemoryPoolMap(ResourceManagerClusterStateProvider provider, int memoryPoolSize, MemoryPoolId memoryPoolId, int assignedQueries, int blockedNodes, int maxBytes, int reservedBytes, int reservedRevocableBytes, Optional<String> largestMemoryQuery)
    {
        Map<MemoryPoolId, ClusterMemoryPoolInfo> memoryPoolMap = provider.getClusterMemoryPoolInfo();
        assertNotNull(memoryPoolMap);
        assertEquals(memoryPoolMap.size(), memoryPoolSize);

        ClusterMemoryPoolInfo clusterMemoryPoolInfo = memoryPoolMap.get(memoryPoolId);
        assertNotNull(clusterMemoryPoolInfo);
        assertEquals(clusterMemoryPoolInfo.getAssignedQueries(), assignedQueries);
        assertEquals(clusterMemoryPoolInfo.getBlockedNodes(), blockedNodes);
        assertEquals(clusterMemoryPoolInfo.getMemoryPoolInfo().getMaxBytes(), maxBytes);
        assertEquals(clusterMemoryPoolInfo.getMemoryPoolInfo().getReservedBytes(), reservedBytes);
        assertEquals(clusterMemoryPoolInfo.getMemoryPoolInfo().getReservedRevocableBytes(), reservedRevocableBytes);
        assertEquals(clusterMemoryPoolInfo.getLargestMemoryQuery().map(QueryId::getId), largestMemoryQuery);
    }

    private static BasicQueryInfo createQueryInfo(String queryId, QueryState state)
    {
        return createQueryInfo(queryId, state, "global", GENERAL_POOL);
    }

    private static BasicQueryInfo createQueryInfo(String queryId, QueryState state, String resourceGroupId, MemoryPoolId memoryPool)
    {
        return createQueryInfo(queryId, state, resourceGroupId, memoryPool, DataSize.valueOf("24GB"));
    }

    private static BasicQueryInfo createQueryInfo(String queryId, QueryState state, String resourceGroupIdString, MemoryPoolId memoryPool, DataSize totalMemoryReservation)
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(Arrays.asList(resourceGroupIdString.split("\\.")));
        return new BasicQueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                Optional.of(resourceGroupId),
                state,
                memoryPool,
                true,
                URI.create("1"),
                "",
                new BasicQueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        Duration.valueOf("6m"),
                        Duration.valueOf("8m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("34m"),
                        11,
                        12,
                        13,
                        14,
                        15,
                        100,
                        DataSize.valueOf("21GB"),
                        22,
                        23,
                        24,
                        DataSize.valueOf("1MB"),
                        totalMemoryReservation,
                        DataSize.valueOf("25GB"),
                        DataSize.valueOf("26GB"),
                        DataSize.valueOf("27GB"),
                        DataSize.valueOf("28GB"),
                        Duration.valueOf("23m"),
                        Duration.valueOf("24m"),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        DataSize.valueOf("123MB"),
                        OptionalDouble.of(20)),
                null,
                Optional.empty(),
                ImmutableList.of());
    }
}
