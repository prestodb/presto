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
package com.facebook.presto.server;

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.memory.MemoryInfo;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.resourcemanager.ResourceManagerClusterStateProvider;
import com.facebook.presto.resourcemanager.ResourceManagerInconsistentException;
import com.facebook.presto.resourcemanager.ResourceManagerServer;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.airlift.units.DataSize.succinctBytes;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.FINISHING;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestResourceManagerServices
{
    @Test
    public void testQueryInfo()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node2", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(nodeManager, createTestingSessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("5s"), Duration.valueOf("0s"), Duration.valueOf("4s"), true, executor);
        ResourceManagerResource resource = new ResourceManagerResource(provider, listeningDecorator(executor));
        ResourceManagerServer server = new ResourceManagerServer(provider, listeningDecorator(executor));

        long query1Sequence = 0;
        long query2Sequence = 0;
        long query3Sequence = 0;
        long query4Sequence = 0;
        server.queryHeartbeat("node1", createQueryInfo("1", QUEUED), query1Sequence);
        resource.queryHeartbeat("node1", createQueryInfo("1", QUEUED), query1Sequence++);

        server.queryHeartbeat("node1", createQueryInfo("2", RUNNING), query2Sequence);
        resource.queryHeartbeat("node1", createQueryInfo("2", RUNNING), query2Sequence++);

        server.queryHeartbeat("node1", createQueryInfo("3", FINISHED), query3Sequence);
        resource.queryHeartbeat("node1", createQueryInfo("3", FINISHED), query3Sequence++);

        server.queryHeartbeat("node1", createQueryInfo("4", FAILED), query4Sequence);
        resource.queryHeartbeat("node1", createQueryInfo("4", FAILED), query4Sequence++);

        assertEquals(resource.getRunningTaskCount().get(), server.getRunningTaskCount().get());

        server.queryHeartbeat("node1", createQueryInfo("1", RUNNING), query1Sequence);
        resource.queryHeartbeat("node1", createQueryInfo("1", RUNNING), query1Sequence++);

        assertEquals(resource.getRunningTaskCount().get(), server.getRunningTaskCount().get());

        server.queryHeartbeat("node1", createQueryInfo("2", FINISHING), query2Sequence);
        resource.queryHeartbeat("node1", createQueryInfo("2", FINISHING), query2Sequence++);

        assertEquals(resource.getRunningTaskCount().get(), server.getRunningTaskCount().get());

        server.queryHeartbeat("node1", createQueryInfo("2", FINISHED), query2Sequence);
        resource.queryHeartbeat("node1", createQueryInfo("2", FINISHED), query2Sequence++);

        assertEquals(resource.getRunningTaskCount().get(), server.getRunningTaskCount().get());
    }

    @Test(timeOut = 15_000)
    public void testResourceGroups()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node1", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node2", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        nodeManager.addNode(new ConnectorId("x"), new InternalNode("node3", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true));
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(nodeManager, createTestingSessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("50s"), Duration.valueOf("0s"), Duration.valueOf("4s"), true, executor);
        ResourceManagerResource resource = new ResourceManagerResource(provider, listeningDecorator(executor));
        ResourceManagerServer server = new ResourceManagerServer(provider, listeningDecorator(executor));

        server.nodeHeartbeat(createCoordinatorNodeStatus("local"));
        server.nodeHeartbeat(createCoordinatorNodeStatus("node1"));
        server.nodeHeartbeat(createCoordinatorNodeStatus("node2"));
        server.nodeHeartbeat(createCoordinatorNodeStatus("node3"));

        resource.nodeHeartbeat(createCoordinatorNodeStatus("local"));
        resource.nodeHeartbeat(createCoordinatorNodeStatus("node1"));
        resource.nodeHeartbeat(createCoordinatorNodeStatus("node2"));
        resource.nodeHeartbeat(createCoordinatorNodeStatus("node3"));

        long query1Sequence = 0;
        long query2Sequence = 0;
        long query3Sequence = 0;
        long query4Sequence = 0;
        long query5Sequence = 0;
        long query6Sequence = 0;
        resource.queryHeartbeat("node1", createQueryInfo("1", QUEUED, "rg1", GENERAL_POOL), query1Sequence);
        server.queryHeartbeat("node1", createQueryInfo("1", QUEUED, "rg1", GENERAL_POOL), query1Sequence++);

        server.queryHeartbeat("node1", createQueryInfo("2", RUNNING, "rg2", GENERAL_POOL), query2Sequence);
        resource.queryHeartbeat("node1", createQueryInfo("2", RUNNING, "rg2", GENERAL_POOL), query2Sequence++);

        resource.queryHeartbeat("node1", createQueryInfo("3", FINISHING, "rg3", GENERAL_POOL), query3Sequence);
        server.queryHeartbeat("node1", createQueryInfo("3", FINISHING, "rg3", GENERAL_POOL), query3Sequence++);

        server.queryHeartbeat("node1", createQueryInfo("4", FINISHED, "rg4", GENERAL_POOL), query4Sequence);
        resource.queryHeartbeat("node1", createQueryInfo("4", FINISHED, "rg4", GENERAL_POOL), query4Sequence++);

        resource.queryHeartbeat("node1", createQueryInfo("5", FAILED, "rg5", GENERAL_POOL), query5Sequence);
        server.queryHeartbeat("node1", createQueryInfo("5", FAILED, "rg5", GENERAL_POOL), query5Sequence++);

        assertResourceGroups(server.getResourceGroupInfo("node1").get(), 0);
        assertResourceGroups(resource.getResourceGroupInfo("node1").get(), 0);
        assertResourceGroups(resource.getResourceGroupInfo("node2").get(), 3);
        assertResourceGroups(resource.getResourceGroupInfo("node2").get(), 3);

        // Add an existing leaf node from another node
        server.queryHeartbeat("node3", createQueryInfo("6", QUEUED, "rg6", GENERAL_POOL), query6Sequence);
        resource.queryHeartbeat("node3", createQueryInfo("6", QUEUED, "rg6", GENERAL_POOL), query6Sequence++);

        assertResourceGroups(server.getResourceGroupInfo("node3").get(), 3);
        assertResourceGroups(resource.getResourceGroupInfo("node3").get(), 3);
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
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        ResourceManagerClusterStateProvider provider = new ResourceManagerClusterStateProvider(nodeManager, createTestingSessionPropertyManager(), 10, Duration.valueOf("4s"), Duration.valueOf("8s"), Duration.valueOf("4s"), Duration.valueOf("0s"), Duration.valueOf("4s"), true, executor);
        ResourceManagerResource resource = new ResourceManagerResource(provider, listeningDecorator(executor));
        ResourceManagerServer server = new ResourceManagerServer(provider, listeningDecorator(executor));

        // Memory pool starts off empty
        assertMemoryPoolMap(resource.getMemoryPoolInfo().get(), 2, GENERAL_POOL, 0, 0, 0, 0, 0, Optional.empty());
        assertMemoryPoolMap(resource.getMemoryPoolInfo().get(), 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());

        assertMemoryPoolMap(server.getMemoryPoolInfo().get(), 2, GENERAL_POOL, 0, 0, 0, 0, 0, Optional.empty());
        assertMemoryPoolMap(server.getMemoryPoolInfo().get(), 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());

        // Create a node and heartbeat to the resource manager
        resource.nodeHeartbeat(createNodeStatus("nodeId", GENERAL_POOL, createMemoryPoolInfo(100, 2, 1)));
        assertMemoryPoolMap(resource.getMemoryPoolInfo().get(), 2, GENERAL_POOL, 0, 0, 100, 2, 1, Optional.empty());
        assertMemoryPoolMap(resource.getMemoryPoolInfo().get(), 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());

        server.nodeHeartbeat(createNodeStatus("nodeId", GENERAL_POOL, createMemoryPoolInfo(100, 2, 1)));
        assertMemoryPoolMap(server.getMemoryPoolInfo().get(), 2, GENERAL_POOL, 0, 0, 100, 2, 1, Optional.empty());
        assertMemoryPoolMap(server.getMemoryPoolInfo().get(), 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());

        // Register a query and heartbeat that to the resource manager
        resource.queryHeartbeat("nodeId1", createQueryInfo("1", QUEUED, "rg4", GENERAL_POOL), query1Sequence++);
        assertMemoryPoolMap(resource.getMemoryPoolInfo().get(), 2, GENERAL_POOL, 1, 0, 100, 2, 1, Optional.of("1"));
        assertMemoryPoolMap(resource.getMemoryPoolInfo().get(), 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());
        server.queryHeartbeat("nodeId1", createQueryInfo("1", QUEUED, "rg4", GENERAL_POOL), query1Sequence++);
        assertMemoryPoolMap(server.getMemoryPoolInfo().get(), 2, GENERAL_POOL, 1, 0, 100, 2, 1, Optional.of("1"));
        assertMemoryPoolMap(server.getMemoryPoolInfo().get(), 2, RESERVED_POOL, 0, 0, 0, 0, 0, Optional.empty());
    }

    private NodeStatus createNodeStatus(String nodeId, MemoryPoolId memoryPoolId, MemoryPoolInfo memoryPoolInfo)
    {
        return new NodeStatus(
                nodeId,
                new NodeVersion("1"),
                "environment",
                false,
                new Duration(1, SECONDS),
                "http://externalAddress",
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

    private void assertResourceGroups(List<ResourceGroupRuntimeInfo> resourceGroups, int count)
            throws ResourceManagerInconsistentException
    {
        assertNotNull(resourceGroups);
        assertEquals(resourceGroups.size(), count);
    }

    private void assertMemoryPoolMap(Map<MemoryPoolId, ClusterMemoryPoolInfo> memoryPoolMap, int memoryPoolSize, MemoryPoolId memoryPoolId, int assignedQueries, int blockedNodes, int maxBytes, int reservedBytes, int reservedRevocableBytes, Optional<String> largestMemoryQuery)
    {
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

    public static BasicQueryInfo createQueryInfo(String queryId, QueryState state)
    {
        return createQueryInfo(queryId, state, "global", GENERAL_POOL);
    }

    private static BasicQueryInfo createQueryInfo(String queryId, QueryState state, String resourceGroupId, MemoryPoolId memoryPool)
    {
        return createQueryInfo(queryId, state, resourceGroupId, memoryPool, DataSize.valueOf("24GB").toBytes());
    }

    private static BasicQueryInfo createQueryInfo(String queryId, QueryState state, String resourceGroupIdString, MemoryPoolId memoryPool, long totalMemoryReservation)
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
                        new DateTime("1991-09-06T05:00").getMillis(),
                        new DateTime("1991-09-06T05:01").getMillis(),
                        Duration.valueOf("6m"),
                        Duration.valueOf("8m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("34m"),
                        Duration.valueOf("10m"),
                        11,
                        12,
                        13,
                        14,
                        15,
                        100,
                        13,
                        14,
                        15,
                        100,
                        13,
                        14,
                        15,
                        100,
                        DataSize.valueOf("21GB"),
                        22,
                        23,
                        24,
                        DataSize.valueOf("1MB"),
                        succinctBytes(totalMemoryReservation),
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
                ImmutableList.of(),
                Optional.empty());
    }
}
