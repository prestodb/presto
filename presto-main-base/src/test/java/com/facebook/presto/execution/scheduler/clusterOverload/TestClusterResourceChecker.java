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
package com.facebook.presto.execution.scheduler.clusterOverload;

import com.facebook.presto.execution.ClusterOverloadConfig;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeState;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClusterResourceChecker
{
    private static final int CACHE_TTL_SECS = 1;
    private static final int SLEEP_BUFFER_MILLIS = 200;

    private ClusterOverloadConfig config;
    private TestingNodeOverloadPolicy nodeOverloadPolicy;
    private ClusterResourceChecker clusterResourceChecker;
    private TestingInternalNodeManager nodeManager;

    @BeforeMethod
    public void setUp()
    {
        config = new ClusterOverloadConfig()
                .setOverloadCheckCacheTtlInSecs(CACHE_TTL_SECS)
                .setClusterOverloadThrottlingEnabled(true);

        nodeOverloadPolicy = new TestingNodeOverloadPolicy();
        nodeManager = new TestingInternalNodeManager();

        clusterResourceChecker = new ClusterResourceChecker(nodeOverloadPolicy, config);
    }

    public void testInitialState()
    {
        assertFalse(clusterResourceChecker.isClusterOverloaded());
        assertEquals(clusterResourceChecker.getOverloadDetectionCount().getTotalCount(), 0);
        assertEquals(clusterResourceChecker.getOverloadDurationMillis(), 0);
        assertTrue(clusterResourceChecker.isClusterOverloadThrottlingEnabled());
    }

    @Test
    public void testCanRunMoreOnCluster()
    {
        // Initially not overloaded
        nodeOverloadPolicy.setOverloaded(false);
        assertTrue(clusterResourceChecker.canRunMoreOnCluster(nodeManager));
        assertEquals(nodeOverloadPolicy.getCheckCount(), 1);

        // Should use cached result
        assertTrue(clusterResourceChecker.canRunMoreOnCluster(nodeManager));
        assertEquals(nodeOverloadPolicy.getCheckCount(), 1); // No additional check

        // Set overloaded but should still use cache
        nodeOverloadPolicy.setOverloaded(true);
        assertTrue(clusterResourceChecker.canRunMoreOnCluster(nodeManager));
        assertEquals(nodeOverloadPolicy.getCheckCount(), 1); // No additional check

        // Wait for cache to expire with buffer time
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);

        // Should check again and return false (overloaded)
        assertFalse(clusterResourceChecker.canRunMoreOnCluster(nodeManager));
        assertEquals(nodeOverloadPolicy.getCheckCount(), 2);
        assertTrue(clusterResourceChecker.isClusterOverloaded());
        assertEquals(clusterResourceChecker.getOverloadDetectionCount().getTotalCount(), 1);

        // Set back to not overloaded
        nodeOverloadPolicy.setOverloaded(false);

        // Should still use cache
        assertFalse(clusterResourceChecker.canRunMoreOnCluster(nodeManager));
        assertEquals(nodeOverloadPolicy.getCheckCount(), 2); // No additional check

        // Wait for cache to expire with buffer time
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);

        // Should check again and return true (not overloaded)
        assertTrue(clusterResourceChecker.canRunMoreOnCluster(nodeManager));
        assertEquals(nodeOverloadPolicy.getCheckCount(), 3);
        assertFalse(clusterResourceChecker.isClusterOverloaded());
    }

    @Test
    public void testOverloadDurationMetric()
    {
        // Set to overloaded
        nodeOverloadPolicy.setOverloaded(true);
        assertFalse(clusterResourceChecker.canRunMoreOnCluster(nodeManager));
        assertTrue(clusterResourceChecker.isClusterOverloaded());

        // Duration should be greater than 0
        sleep(100);
        assertTrue(clusterResourceChecker.getOverloadDurationMillis() > 0);

        // Set back to not overloaded
        nodeOverloadPolicy.setOverloaded(false);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertTrue(clusterResourceChecker.canRunMoreOnCluster(nodeManager));

        // Duration should be 0 again
        assertEquals(clusterResourceChecker.getOverloadDurationMillis(), 0);
    }

    @Test
    public void testMultipleOverloadTransitions()
    {
        // First transition to overloaded
        nodeOverloadPolicy.setOverloaded(true);
        assertFalse(clusterResourceChecker.canRunMoreOnCluster(nodeManager));
        assertEquals(clusterResourceChecker.getOverloadDetectionCount().getTotalCount(), 1);

        // Wait for cache to expire and transition back to not overloaded
        nodeOverloadPolicy.setOverloaded(false);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertTrue(clusterResourceChecker.canRunMoreOnCluster(nodeManager));

        // Second transition to overloaded
        nodeOverloadPolicy.setOverloaded(true);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertFalse(clusterResourceChecker.canRunMoreOnCluster(nodeManager));
        assertEquals(clusterResourceChecker.getOverloadDetectionCount().getTotalCount(), 2);
    }

    @Test
    public void testClusterOverloadThrottlingEnabled()
    {
        // Default is enabled (set in setUp)
        assertTrue(clusterResourceChecker.isClusterOverloadThrottlingEnabled());

        // Create a new config with throttling disabled
        ClusterOverloadConfig disabledConfig = new ClusterOverloadConfig()
                .setOverloadCheckCacheTtlInSecs(CACHE_TTL_SECS)
                .setClusterOverloadThrottlingEnabled(false);

        // Create a new checker with throttling disabled
        ClusterResourceChecker disabledChecker = new ClusterResourceChecker(nodeOverloadPolicy, disabledConfig);
        assertFalse(disabledChecker.isClusterOverloadThrottlingEnabled());

        // Even when cluster is overloaded, canRunMoreOnCluster should return true if throttling is disabled
        nodeOverloadPolicy.setOverloaded(true);
        assertTrue(disabledChecker.canRunMoreOnCluster(nodeManager));
    }

    private void sleep(long millis)
    {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static class TestingNodeOverloadPolicy
            implements NodeOverloadPolicy
    {
        private boolean overloaded;
        private final AtomicInteger checkCount = new AtomicInteger();

        @Override
        public boolean isClusterOverloaded(InternalNodeManager nodeManager)
        {
            checkCount.incrementAndGet();
            return overloaded;
        }

        @Override
        public String getName()
        {
            return "test-policy";
        }

        public void setOverloaded(boolean overloaded)
        {
            this.overloaded = overloaded;
        }

        public int getCheckCount()
        {
            return checkCount.get();
        }
    }

    private static class TestingInternalNodeManager
            implements InternalNodeManager
    {
        @Override
        public Set<InternalNode> getNodes(NodeState state)
        {
            return Collections.emptySet();
        }

        @Override
        public Set<InternalNode> getActiveConnectorNodes(ConnectorId connectorId)
        {
            return Collections.emptySet();
        }

        @Override
        public Set<InternalNode> getAllConnectorNodes(ConnectorId connectorId)
        {
            return Collections.emptySet();
        }

        @Override
        public InternalNode getCurrentNode()
        {
            return null;
        }

        @Override
        public Set<InternalNode> getCoordinators()
        {
            return Collections.emptySet();
        }

        @Override
        public Set<InternalNode> getShuttingDownCoordinator()
        {
            return Collections.emptySet();
        }

        @Override
        public Set<InternalNode> getResourceManagers()
        {
            return Collections.emptySet();
        }

        @Override
        public Set<InternalNode> getCatalogServers()
        {
            return Collections.emptySet();
        }

        @Override
        public Set<InternalNode> getCoordinatorSidecars()
        {
            return Collections.emptySet();
        }

        @Override
        public AllNodes getAllNodes()
        {
            return new AllNodes(
                    Collections.emptySet(),
                    Collections.emptySet(),
                    Collections.emptySet(),
                    Collections.emptySet(),
                    Collections.emptySet(),
                    Collections.emptySet(),
                    Collections.emptySet());
        }

        @Override
        public void refreshNodes()
        {
            // No-op for testing
        }

        @Override
        public void addNodeChangeListener(Consumer<AllNodes> listener)
        {
            // No-op for testing
        }

        @Override
        public void removeNodeChangeListener(Consumer<AllNodes> listener)
        {
            // No-op for testing
        }

        @Override
        public Map<String, Double> getNodeLoadMetrics(String nodeIdentifier)
        {
            return Collections.emptyMap();
        }
    }
}
