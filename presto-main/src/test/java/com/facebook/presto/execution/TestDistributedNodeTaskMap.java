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
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.resourcemanager.NodeTaskState;
import com.facebook.presto.resourcemanager.ResourceManagerClient;
import com.facebook.presto.resourcemanager.ResourceManagerConfig;
import com.facebook.presto.resourcemanager.ResourceManagerInconsistentException;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestDistributedNodeTaskMap
{
    private DistributedNodeTaskMap distributedNodeTaskMap;
    private ScheduledExecutorService executorService;
    private TestResourceManagerClient client;

    @BeforeMethod
    public void setUp()
    {
        client = new TestResourceManagerClient();
        ResourceManagerConfig resourceManagerConfig = new ResourceManagerConfig();
        resourceManagerConfig.setNodeTaskMapFetchInterval(new Duration(50, MILLISECONDS));
        executorService = Executors.newSingleThreadScheduledExecutor();
        distributedNodeTaskMap = new DistributedNodeTaskMap((u, v) -> client, executorService, resourceManagerConfig);
        distributedNodeTaskMap.init();
    }

    @AfterMethod
    public void tearDown()
    {
        executorService.shutdownNow();
        if (distributedNodeTaskMap != null) {
            distributedNodeTaskMap.stop();
            distributedNodeTaskMap = null;
        }
    }

    @Test(timeOut = 10_000)
    public void testReporting()
            throws Exception
    {
        InternalNode node = new InternalNode("nodeId", URI.create("http://fake.invalid/"), new NodeVersion("version"), false);

        client.putNodeTaskState(node.getNodeIdentifier(), new NodeTaskState(new PartitionedSplitsInfo(1, 10), 100, 0.1));
        client.waitForAccesses(2, 10, SECONDS);

        assertEquals(distributedNodeTaskMap.getPartitionedSplitsOnNode(node), PartitionedSplitsInfo.forSplitCountAndWeightSum(1, 10));
        assertEquals(distributedNodeTaskMap.getNodeTotalMemoryUsageInBytes(node), 100);
        assertEquals(distributedNodeTaskMap.getNodeCpuUtilizationPercentage(node), 0.1);
    }

    @Test(timeOut = 10_000)
    public void testMultipleUpdates()
            throws Exception
    {
        InternalNode node = new InternalNode("nodeId", URI.create("http://fake.invalid/"), new NodeVersion("version"), false);
        InternalNode node2 = new InternalNode("nodeId2", URI.create("http://fake.invalid/"), new NodeVersion("version"), false);

        client.putNodeTaskState(node.getNodeIdentifier(), new NodeTaskState(new PartitionedSplitsInfo(1, 10), 100, 0.1));
        client.waitForAccesses(2, 10, SECONDS);

        assertEquals(distributedNodeTaskMap.getPartitionedSplitsOnNode(node), PartitionedSplitsInfo.forSplitCountAndWeightSum(1, 10));
        assertEquals(distributedNodeTaskMap.getNodeTotalMemoryUsageInBytes(node), 100);
        assertEquals(distributedNodeTaskMap.getNodeCpuUtilizationPercentage(node), 0.1);

        client.putNodeTaskState("nodeId", new NodeTaskState(new PartitionedSplitsInfo(1000, -1), 200, 0.5));
        client.putNodeTaskState("nodeId2", new NodeTaskState(new PartitionedSplitsInfo(800, 456), 160, 0.9));
        client.waitForAccesses(2, 10, SECONDS);

        assertEquals(distributedNodeTaskMap.getPartitionedSplitsOnNode(node), PartitionedSplitsInfo.forSplitCountAndWeightSum(1000, -1));
        assertEquals(distributedNodeTaskMap.getNodeTotalMemoryUsageInBytes(node), 200);
        assertEquals(distributedNodeTaskMap.getNodeCpuUtilizationPercentage(node), 0.5);
        assertEquals(distributedNodeTaskMap.getPartitionedSplitsOnNode(node2), PartitionedSplitsInfo.forSplitCountAndWeightSum(800, 456));
        assertEquals(distributedNodeTaskMap.getNodeTotalMemoryUsageInBytes(node2), 160);
        assertEquals(distributedNodeTaskMap.getNodeCpuUtilizationPercentage(node2), 0.9);
    }

    private static final class TestResourceManagerClient
            implements ResourceManagerClient
    {
        private final ConcurrentMap<String, NodeTaskState> nodeTaskStateMap = new ConcurrentHashMap<>();

        private final Lock lock = new ReentrantLock();
        private final Condition accessed = lock.newCondition();

        private int expectedNumberOfAccesses;
        private int numberOfAccesses;

        public void putNodeTaskState(String nodeId, NodeTaskState nodeTaskState)
        {
            requireNonNull(nodeId, "nodeId is null");
            requireNonNull(nodeTaskState, "nodeTaskState is null");
            lock.lock();
            try {
                nodeTaskStateMap.put(nodeId, nodeTaskState);
            }
            finally {
                lock.unlock();
            }
        }

        public void waitForAccesses(int expectedNumberOfAccesses, long timeout, TimeUnit unit)
                throws InterruptedException
        {
            requireNonNull(unit, "unit is null");
            lock.lock();
            try {
                this.expectedNumberOfAccesses = expectedNumberOfAccesses;
                numberOfAccesses = 0;

                accessed.await(timeout, unit);
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public Map<String, NodeTaskState> getNodeTaskStates()
        {
            lock.lock();
            try {
                numberOfAccesses += 1;
                if (numberOfAccesses >= expectedNumberOfAccesses) {
                    accessed.signal();
                }
                return nodeTaskStateMap;
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public void queryHeartbeat(String internalNode, BasicQueryInfo basicQueryInfo, long sequenceId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<ResourceGroupRuntimeInfo> getResourceGroupInfo(String excludingNode)
                throws ResourceManagerInconsistentException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nodeHeartbeat(NodeStatus nodeStatus)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<MemoryPoolId, ClusterMemoryPoolInfo> getMemoryPoolInfo()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resourceGroupRuntimeHeartbeat(String node, List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getRunningTaskCount()
        {
            throw new UnsupportedOperationException();
        }
    }
}
