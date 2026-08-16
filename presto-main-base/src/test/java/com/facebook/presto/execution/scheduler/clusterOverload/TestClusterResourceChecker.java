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
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
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
    private static final ResourceGroupId TEST_RESOURCE_GROUP = new ResourceGroupId(ImmutableList.of("test", "group"));

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
        assertFalse(clusterResourceChecker.isClusterCurrentlyOverloaded(TEST_RESOURCE_GROUP));

        // Wait for periodic check to update state
        clusterOverloadPolicy.setOverloaded(true);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertTrue(clusterResourceChecker.isClusterCurrentlyOverloaded(TEST_RESOURCE_GROUP));
        assertTrue(clusterResourceChecker.isClusterOverloaded());
        assertEquals(clusterResourceChecker.getOverloadDetectionCount().getTotalCount(), 1);

        // Set back to not overloaded
        clusterOverloadPolicy.setOverloaded(false);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertFalse(clusterResourceChecker.isClusterCurrentlyOverloaded(TEST_RESOURCE_GROUP));
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
        assertTrue(clusterResourceChecker.isClusterCurrentlyOverloaded(TEST_RESOURCE_GROUP));
        assertTrue(clusterResourceChecker.isClusterOverloaded());

        // Duration should be greater than 0
        sleep(100);
        assertTrue(clusterResourceChecker.getOverloadDurationMillis() > 0);

        // Set back to not overloaded
        clusterOverloadPolicy.setOverloaded(false);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertFalse(clusterResourceChecker.isClusterCurrentlyOverloaded(TEST_RESOURCE_GROUP));

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
        assertTrue(clusterResourceChecker.isClusterCurrentlyOverloaded(TEST_RESOURCE_GROUP));
        assertEquals(clusterResourceChecker.getOverloadDetectionCount().getTotalCount(), 1);

        // Wait and transition back to not overloaded
        clusterOverloadPolicy.setOverloaded(false);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertFalse(clusterResourceChecker.isClusterCurrentlyOverloaded(TEST_RESOURCE_GROUP));

        // Second transition to overloaded
        clusterOverloadPolicy.setOverloaded(true);
        sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
        assertTrue(clusterResourceChecker.isClusterCurrentlyOverloaded(TEST_RESOURCE_GROUP));
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
        assertFalse(disabledChecker.isClusterCurrentlyOverloaded(TEST_RESOURCE_GROUP));
    }

    @Test
    public void testBypassResourceGroupsSkipsThrottling()
    {
        // Bypass on path semantics: a group bypasses cluster overload when it is equal to,
        // an ancestor of, or a descendant of any configured bypass entry. Required because
        // canRunMore() is invoked at every ancestor in the hierarchy during admission.
        ClusterOverloadConfig bypassConfig = new ClusterOverloadConfig()
                .setOverloadCheckCacheTtlInSecs(CACHE_TTL_SECS)
                .setClusterOverloadThrottlingEnabled(true)
                .setThrottlingBypassResourceGroups("global.admin, global.etl.priority");

        ClusterResourceChecker bypassChecker = new ClusterResourceChecker(clusterOverloadPolicy, bypassConfig, nodeManager);
        bypassChecker.start();

        try {
            // Drive the cluster into the overloaded state
            clusterOverloadPolicy.setOverloaded(true);
            sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
            assertTrue(bypassChecker.isClusterOverloaded());

            ResourceGroupId bypassedExact = new ResourceGroupId(ImmutableList.of("global", "admin"));
            ResourceGroupId alsoBypassedExact = new ResourceGroupId(ImmutableList.of("global", "etl", "priority"));
            ResourceGroupId descendantOfBypassed = new ResourceGroupId(ImmutableList.of("global", "admin", "user_x"));
            ResourceGroupId deepDescendantOfBypassed = new ResourceGroupId(ImmutableList.of("global", "admin", "user_x", "session_1"));
            ResourceGroupId rootAncestor = new ResourceGroupId(ImmutableList.of("global"));
            ResourceGroupId etlIntermediateAncestor = new ResourceGroupId(ImmutableList.of("global", "etl"));
            ResourceGroupId siblingOfBypassed = new ResourceGroupId(ImmutableList.of("global", "adhoc"));
            ResourceGroupId siblingUnderEtl = new ResourceGroupId(ImmutableList.of("global", "etl", "lowprio"));

            // Exact match bypasses
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(bypassedExact));
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(alsoBypassedExact));

            // Descendants of a bypassed group bypass (so the leaf does not veto its own admission)
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(descendantOfBypassed));
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(deepDescendantOfBypassed));

            // Ancestors of a bypassed group bypass (so they do not veto a downstream bypassed admission)
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(rootAncestor));
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(etlIntermediateAncestor));

            // Unrelated subtrees remain throttled
            assertTrue(bypassChecker.isClusterCurrentlyOverloaded(siblingOfBypassed));
            // Sibling under an ancestor of a bypassed group: 'global.etl' is bypassed (ancestor of priority),
            // but 'global.etl.lowprio' is neither equal to, ancestor of, nor descendant of any bypass entry,
            // so its leaf-level check still vetoes — which is the correct behavior for queries on that branch.
            assertTrue(bypassChecker.isClusterCurrentlyOverloaded(siblingUnderEtl));

            // Bypass counter incremented once per matched call above (6 matches)
            assertEquals(bypassChecker.getThrottlingBypassCount().getTotalCount(), 6);
        }
        finally {
            bypassChecker.stop();
        }
    }

    @Test
    public void testBypassNeverReturnsFalseWhenClusterHealthy()
    {
        ClusterOverloadConfig bypassConfig = new ClusterOverloadConfig()
                .setOverloadCheckCacheTtlInSecs(CACHE_TTL_SECS)
                .setClusterOverloadThrottlingEnabled(true)
                .setThrottlingBypassResourceGroups("global.admin");

        ClusterResourceChecker bypassChecker = new ClusterResourceChecker(clusterOverloadPolicy, bypassConfig, nodeManager);

        // Cluster healthy — bypass logic must not run / increment counter
        ResourceGroupId bypassed = new ResourceGroupId(ImmutableList.of("global", "admin"));
        assertFalse(bypassChecker.isClusterCurrentlyOverloaded(bypassed));
        assertEquals(bypassChecker.getThrottlingBypassCount().getTotalCount(), 0);
    }

    @Test
    public void testBypassedSiblingDoesNotLeakBypassToOtherSibling()
    {
        // Bypass scenario: bypass list contains 'global.pipeline.a' only. A query in
        // 'global.pipeline.b' must remain throttled, even though both share the ancestors
        // 'global' and 'global.pipeline' (which themselves bypass because they are ancestors
        // of the configured 'a'). The leaf-level check on 'b' is the veto that keeps 'b'
        // throttled while 'a' is admitted.
        ClusterOverloadConfig bypassConfig = new ClusterOverloadConfig()
                .setOverloadCheckCacheTtlInSecs(CACHE_TTL_SECS)
                .setClusterOverloadThrottlingEnabled(true)
                .setThrottlingBypassResourceGroups("global.pipeline.a");

        ClusterResourceChecker bypassChecker = new ClusterResourceChecker(clusterOverloadPolicy, bypassConfig, nodeManager);
        bypassChecker.start();

        try {
            clusterOverloadPolicy.setOverloaded(true);
            sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
            assertTrue(bypassChecker.isClusterOverloaded());

            ResourceGroupId pipelineA = new ResourceGroupId(ImmutableList.of("global", "pipeline", "a"));
            ResourceGroupId pipelineB = new ResourceGroupId(ImmutableList.of("global", "pipeline", "b"));
            ResourceGroupId sharedParent = new ResourceGroupId(ImmutableList.of("global", "pipeline"));
            ResourceGroupId sharedRoot = new ResourceGroupId(ImmutableList.of("global"));

            // 'a' is bypassed at every level of its admission path.
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(pipelineA));
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(sharedParent));
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(sharedRoot));

            // 'b' is throttled at its own leaf, even though it shares ancestors with 'a'.
            assertTrue(bypassChecker.isClusterCurrentlyOverloaded(pipelineB));
        }
        finally {
            bypassChecker.stop();
        }
    }

    @Test
    public void testBypassBehaviorAcrossOverloadTransitions()
    {
        // Verifies bypass behavior across healthy -> overloaded -> healthy -> overloaded transitions:
        // - Under load, bypassed groups skip throttling while unrelated groups are throttled.
        // - When the cluster becomes healthy, the early-exit in isClusterCurrentlyOverloaded must
        //   short-circuit before the bypass path, so the bypass counter must not increment for
        //   either group while healthy.
        // - Re-entering overload restores bypass behavior and resumes counter increments.
        ClusterOverloadConfig bypassConfig = new ClusterOverloadConfig()
                .setOverloadCheckCacheTtlInSecs(CACHE_TTL_SECS)
                .setClusterOverloadThrottlingEnabled(true)
                .setThrottlingBypassResourceGroups("global.admin");

        ClusterResourceChecker bypassChecker = new ClusterResourceChecker(clusterOverloadPolicy, bypassConfig, nodeManager);
        bypassChecker.start();

        try {
            ResourceGroupId bypassed = new ResourceGroupId(ImmutableList.of("global", "admin"));
            ResourceGroupId throttled = new ResourceGroupId(ImmutableList.of("global", "adhoc"));

            // Initial healthy state: nothing is throttled, bypass counter untouched.
            clusterOverloadPolicy.setOverloaded(false);
            sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
            assertFalse(bypassChecker.isClusterOverloaded());
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(bypassed));
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(throttled));
            assertEquals(bypassChecker.getThrottlingBypassCount().getTotalCount(), 0);

            // Transition healthy -> overloaded: bypassed group skips throttling, unrelated group is throttled.
            clusterOverloadPolicy.setOverloaded(true);
            sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
            assertTrue(bypassChecker.isClusterOverloaded());
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(bypassed));
            assertTrue(bypassChecker.isClusterCurrentlyOverloaded(throttled));
            assertEquals(bypassChecker.getThrottlingBypassCount().getTotalCount(), 1);

            // Transition overloaded -> healthy: nothing is throttled, bypass counter must NOT increment
            // (the cluster-healthy short-circuit must run before the bypass path).
            clusterOverloadPolicy.setOverloaded(false);
            sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
            assertFalse(bypassChecker.isClusterOverloaded());
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(bypassed));
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(throttled));
            assertEquals(bypassChecker.getThrottlingBypassCount().getTotalCount(), 1);

            // Re-enter overload: bypass behavior recovers and counter resumes incrementing.
            clusterOverloadPolicy.setOverloaded(true);
            sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);
            assertTrue(bypassChecker.isClusterOverloaded());
            assertFalse(bypassChecker.isClusterCurrentlyOverloaded(bypassed));
            assertTrue(bypassChecker.isClusterCurrentlyOverloaded(throttled));
            assertEquals(bypassChecker.getThrottlingBypassCount().getTotalCount(), 2);
        }
        finally {
            bypassChecker.stop();
        }
    }

    @Test
    public void testEmptyBypassListBehavesLikeBefore()
    {
        // Default bypass list is empty
        clusterResourceChecker.start();

        try {
            clusterOverloadPolicy.setOverloaded(true);
            sleep((CACHE_TTL_SECS * 1000) + SLEEP_BUFFER_MILLIS);

            ResourceGroupId anyGroup = new ResourceGroupId(ImmutableList.of("global", "admin"));
            assertTrue(clusterResourceChecker.isClusterCurrentlyOverloaded(anyGroup));
            assertEquals(clusterResourceChecker.getThrottlingBypassCount().getTotalCount(), 0);
        }
        finally {
            clusterResourceChecker.stop();
        }
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
