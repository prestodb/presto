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
import com.facebook.presto.spi.NodeLoadMetrics;
import com.facebook.presto.spi.NodeState;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;
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
    private TestingClusterOverloadPolicy clusterOverloadPolicy;
    private ClusterResourceChecker clusterResourceChecker;
    private TestingInternalNodeManager nodeManager;

    @BeforeMethod
    public void setUp()
    {
        config = new ClusterOverloadConfig()
                .setOverloadCheckCacheTtlInSecs(CACHE_TTL_SECS)
                .setClusterOverloadThrottlingEnabled(true);

        clusterOverloadPolicy = new TestingClusterOverloadPolicy();
        nodeManager = new TestingInternalNodeManager();

        clusterResourceChecker = new ClusterResourceChecker(clusterOverloadPolicy, config, nodeManager);
    }

    public void testInitialState()
    {
        assertFalse(clusterResourceChecker.isClusterOverloaded());
        assertEquals(clusterResourceChecker.getOverloadDetectionCount().getTotalCount(), 0);
        assertEquals(clusterResourceChecker.getOverloadDurationMillis(), 0);
        assertTrue(clusterResourceChecker.isClusterOverloadThrottlingEnabled());
    }

    @Test
    public void testIsClusterCurrentlyOverloaded()
    {
        // Start the periodic task
        clusterResourceChecker.start();

        // Initially not overloaded
        clusterOverloadPolicy.setOverloaded(false);
        assertFalse(clusterResourceChecker.isClusterCurrentlyOverloaded());

        // Wait for periodic check to update state
        clusterOverloadPolicy.setOverloaded(true);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertTrue(clusterResourceChecker.isClusterCurrentlyOverloaded());
        assertTrue(clusterResourceChecker.isClusterOverloaded());
        assertEquals(clusterResourceChecker.getOverloadDetectionCount().getTotalCount(), 1);

        // Set back to not overloaded
        clusterOverloadPolicy.setOverloaded(false);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertFalse(clusterResourceChecker.isClusterCurrentlyOverloaded());
        assertFalse(clusterResourceChecker.isClusterOverloaded());

        // Stop the periodic task
        clusterResourceChecker.stop();
    }

    @Test
    public void testOverloadDurationMetric()
    {
        // Start the periodic task
        clusterResourceChecker.start();

        // Set to overloaded and wait for periodic check
        clusterOverloadPolicy.setOverloaded(true);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertTrue(clusterResourceChecker.isClusterCurrentlyOverloaded());
        assertTrue(clusterResourceChecker.isClusterOverloaded());

        // Duration should be greater than 0
        sleep(100);
        assertTrue(clusterResourceChecker.getOverloadDurationMillis() > 0);

        // Set back to not overloaded
        clusterOverloadPolicy.setOverloaded(false);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertFalse(clusterResourceChecker.isClusterCurrentlyOverloaded());

        // Duration should be 0 again
        assertEquals(clusterResourceChecker.getOverloadDurationMillis(), 0);

        // Stop the periodic task
        clusterResourceChecker.stop();
    }

    @Test
    public void testMultipleOverloadTransitions()
    {
        // Start the periodic task
        clusterResourceChecker.start();

        // First transition to overloaded
        clusterOverloadPolicy.setOverloaded(true);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertTrue(clusterResourceChecker.isClusterCurrentlyOverloaded());
        assertEquals(clusterResourceChecker.getOverloadDetectionCount().getTotalCount(), 1);

        // Wait and transition back to not overloaded
        clusterOverloadPolicy.setOverloaded(false);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertFalse(clusterResourceChecker.isClusterCurrentlyOverloaded());

        // Second transition to overloaded
        clusterOverloadPolicy.setOverloaded(true);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertTrue(clusterResourceChecker.isClusterCurrentlyOverloaded());
        assertEquals(clusterResourceChecker.getOverloadDetectionCount().getTotalCount(), 2);

        // Stop the periodic task
        clusterResourceChecker.stop();
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
        ClusterResourceChecker disabledChecker = new ClusterResourceChecker(clusterOverloadPolicy, disabledConfig, nodeManager);
        assertFalse(disabledChecker.isClusterOverloadThrottlingEnabled());

        // Even when cluster is overloaded, isClusterCurrentlyOverloaded should return false if throttling is disabled
        clusterOverloadPolicy.setOverloaded(true);
        assertFalse(disabledChecker.isClusterCurrentlyOverloaded());
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

    private static class TestingClusterOverloadPolicy
            implements ClusterOverloadPolicy
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
        public Optional<NodeLoadMetrics> getNodeLoadMetrics(String nodeIdentifier)
        {
            return Optional.empty();
        }
    }
}
