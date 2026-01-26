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

package com.facebook.presto.execution.resourceGroups;

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.execution.ClusterOverloadConfig;
import com.facebook.presto.execution.MockManagedQueryExecution;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.scheduler.clusterOverload.ClusterResourceChecker;
import com.facebook.presto.execution.scheduler.clusterOverload.CpuMemoryOverloadPolicy;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestInternalResourceGroupManager
{
    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Presto server is still initializing.*")
    public void testQueryFailsWithInitializingConfigurationManager()
    {
        InternalResourceGroupManager<ImmutableMap<Object, Object>> internalResourceGroupManager = new InternalResourceGroupManager<>((poolId, listener) -> {}, new QueryManagerConfig(), new NodeInfo("test"), new MBeanExporter(new TestingMBeanServer()), () -> null, new ServerConfig(), new InMemoryNodeManager(), new ClusterResourceChecker(new CpuMemoryOverloadPolicy(new ClusterOverloadConfig()), new ClusterOverloadConfig(), new InMemoryNodeManager()));
        internalResourceGroupManager.submit(new MockManagedQueryExecution(0), new SelectionContext<>(new ResourceGroupId("global"), ImmutableMap.of()), command -> {});
    }

    @Test
    public void testQuerySucceedsWhenConfigurationManagerLoaded()
            throws Exception
    {
        InternalResourceGroupManager<ImmutableMap<Object, Object>> internalResourceGroupManager = new InternalResourceGroupManager<>((poolId, listener) -> {},
                new QueryManagerConfig(), new NodeInfo("test"), new MBeanExporter(new TestingMBeanServer()), () -> null, new ServerConfig(), new InMemoryNodeManager(), new ClusterResourceChecker(new CpuMemoryOverloadPolicy(new ClusterOverloadConfig()), new ClusterOverloadConfig(), new InMemoryNodeManager()));
        internalResourceGroupManager.loadConfigurationManager();
        internalResourceGroupManager.submit(new MockManagedQueryExecution(0), new SelectionContext<>(new ResourceGroupId("global"), ImmutableMap.of()), command -> {});
    }

    // Tests that admission always succeeds when pacing is disabled (default config)
    @Test
    public void testAdmissionPacingUnlimited()
    {
        // When maxQueryAdmissionsPerSecond is Integer.MAX_VALUE (default), admission should always succeed
        QueryManagerConfig config = new QueryManagerConfig();
        InternalResourceGroupManager<ImmutableMap<Object, Object>> manager = new InternalResourceGroupManager<>(
                (poolId, listener) -> {},
                config,
                new NodeInfo("test"),
                new MBeanExporter(new TestingMBeanServer()),
                () -> null,
                new ServerConfig(),
                new InMemoryNodeManager(),
                new ClusterResourceChecker(new CpuMemoryOverloadPolicy(new ClusterOverloadConfig()), new ClusterOverloadConfig(), new InMemoryNodeManager()));

        // Multiple consecutive calls should all succeed
        assertTrue(manager.tryAcquireAdmissionSlot());
        assertTrue(manager.tryAcquireAdmissionSlot());
        assertTrue(manager.tryAcquireAdmissionSlot());
    }

    // Tests that admission respects 1 query/second rate limit
    @Test
    public void testAdmissionPacingOnePerSecond()
            throws InterruptedException
    {
        // When maxQueryAdmissionsPerSecond is 1, verify admission succeeds after waiting
        QueryManagerConfig config = new QueryManagerConfig().setMaxQueryAdmissionsPerSecond(1);
        InternalResourceGroupManager<ImmutableMap<Object, Object>> manager = new InternalResourceGroupManager<>(
                (poolId, listener) -> {},
                config,
                new NodeInfo("test"),
                new MBeanExporter(new TestingMBeanServer()),
                () -> null,
                new ServerConfig(),
                new InMemoryNodeManager(),
                new ClusterResourceChecker(new CpuMemoryOverloadPolicy(new ClusterOverloadConfig()), new ClusterOverloadConfig(), new InMemoryNodeManager()));

        // First admission should succeed
        assertTrue(manager.tryAcquireAdmissionSlot());

        // Wait for 1 second (required interval) and verify next admission succeeds
        Thread.sleep(1100);
        assertTrue(manager.tryAcquireAdmissionSlot());
    }

    // Tests that admission respects 10 queries/second rate limit
    @Test
    public void testAdmissionPacingMultiplePerSecond()
            throws InterruptedException
    {
        // When maxQueryAdmissionsPerSecond is 10, verify admission succeeds after waiting appropriate interval
        QueryManagerConfig config = new QueryManagerConfig().setMaxQueryAdmissionsPerSecond(10);
        InternalResourceGroupManager<ImmutableMap<Object, Object>> manager = new InternalResourceGroupManager<>(
                (poolId, listener) -> {},
                config,
                new NodeInfo("test"),
                new MBeanExporter(new TestingMBeanServer()),
                () -> null,
                new ServerConfig(),
                new InMemoryNodeManager(),
                new ClusterResourceChecker(new CpuMemoryOverloadPolicy(new ClusterOverloadConfig()), new ClusterOverloadConfig(), new InMemoryNodeManager()));

        // First admission should succeed
        assertTrue(manager.tryAcquireAdmissionSlot());

        // Wait for 150ms (more than the 100ms interval required for 10 queries/sec) and verify next admission succeeds
        Thread.sleep(150);
        assertTrue(manager.tryAcquireAdmissionSlot());
    }

    // Tests that pacing is bypassed when running queries are below threshold
    @Test
    public void testAdmissionPacingBypassedBelowRunningQueryThreshold()
            throws Exception
    {
        // Configure pacing with a threshold of 5 running queries
        // When running queries are below threshold, pacing should be bypassed
        QueryManagerConfig config = new QueryManagerConfig()
                .setMaxQueryAdmissionsPerSecond(1)  // Very slow pacing: 1 per second
                .setMinRunningQueriesForPacing(5);  // Threshold of 5 running queries

        InternalResourceGroupManager<ImmutableMap<Object, Object>> manager = new InternalResourceGroupManager<>(
                (poolId, listener) -> {},
                config,
                new NodeInfo("test"),
                new MBeanExporter(new TestingMBeanServer()),
                () -> null,
                new ServerConfig(),
                new InMemoryNodeManager(),
                new ClusterResourceChecker(new CpuMemoryOverloadPolicy(new ClusterOverloadConfig()), new ClusterOverloadConfig(), new InMemoryNodeManager()));

        manager.loadConfigurationManager();

        // Create a resource group with some running queries (but below threshold)
        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        manager.submit(query1, new SelectionContext<>(new ResourceGroupId("global"), ImmutableMap.of()), directExecutor());
        manager.submit(query2, new SelectionContext<>(new ResourceGroupId("global"), ImmutableMap.of()), directExecutor());

        // With only 2 running queries (below threshold of 5), pacing should be bypassed
        // Multiple rapid admissions should all succeed without waiting
        assertTrue(manager.tryAcquireAdmissionSlot());
        assertTrue(manager.tryAcquireAdmissionSlot());
        assertTrue(manager.tryAcquireAdmissionSlot());

        // Verify metrics are NOT tracked when pacing is bypassed
        assertEquals(manager.getTotalAdmissionAttempts(), 0);
        assertEquals(manager.getTotalAdmissionsGranted(), 0);
        assertEquals(manager.getTotalAdmissionsDenied(), 0);
    }

    // Tests that pacing is enforced when running queries exceed threshold
    @Test
    public void testAdmissionPacingAppliedAboveRunningQueryThreshold()
            throws Exception
    {
        // Configure pacing with a threshold of 2 running queries
        QueryManagerConfig config = new QueryManagerConfig()
                .setMaxQueryAdmissionsPerSecond(1)  // 1 per second
                .setMinRunningQueriesForPacing(2);  // Threshold of 2 running queries

        InternalResourceGroupManager<ImmutableMap<Object, Object>> manager = new InternalResourceGroupManager<>(
                (poolId, listener) -> {},
                config,
                new NodeInfo("test"),
                new MBeanExporter(new TestingMBeanServer()),
                () -> null,
                new ServerConfig(),
                new InMemoryNodeManager(),
                new ClusterResourceChecker(new CpuMemoryOverloadPolicy(new ClusterOverloadConfig()), new ClusterOverloadConfig(), new InMemoryNodeManager()));

        manager.loadConfigurationManager();

        // Create resource groups with enough running queries to exceed threshold
        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
        manager.submit(query1, new SelectionContext<>(new ResourceGroupId("global"), ImmutableMap.of()), directExecutor());
        manager.submit(query2, new SelectionContext<>(new ResourceGroupId("global"), ImmutableMap.of()), directExecutor());
        manager.submit(query3, new SelectionContext<>(new ResourceGroupId("global"), ImmutableMap.of()), directExecutor());

        // Wait for rate limit window to expire after query submissions (which internally call tryAcquireAdmissionSlot)
        Thread.sleep(1100);

        // With 3 running queries (above threshold of 2), pacing should be applied
        // First admission should succeed
        assertTrue(manager.tryAcquireAdmissionSlot());

        // Immediate second attempt should be denied (need to wait 1 second)
        assertFalse(manager.tryAcquireAdmissionSlot());

        // Verify metrics ARE tracked when pacing is applied
        // Note: Query 3's submission also triggered pacing (running queries = 2 at submission time),
        // so we have 3 total attempts: 1 from query3 submission + 2 from explicit calls
        assertEquals(manager.getTotalAdmissionAttempts(), 3);
        assertEquals(manager.getTotalAdmissionsGranted(), 2);
        assertEquals(manager.getTotalAdmissionsDenied(), 1);
    }

    // Tests that pacing turns off when running queries drop below the threshold
    @Test
    public void testAdmissionPacingTurnsOffWhenRunningQueriesDropBelowThreshold()
            throws Exception
    {
        // Configure pacing with a threshold of 2 running queries and a slow rate
        QueryManagerConfig config = new QueryManagerConfig()
                .setMaxQueryAdmissionsPerSecond(1)  // 1 per second, so pacing should be visible
                .setMinRunningQueriesForPacing(2);  // Threshold of 2 running queries

        InternalResourceGroupManager<ImmutableMap<Object, Object>> manager = new InternalResourceGroupManager<>(
                (poolId, listener) -> {},
                config,
                new NodeInfo("test"),
                new MBeanExporter(new TestingMBeanServer()),
                () -> null,
                new ServerConfig(),
                new InMemoryNodeManager(),
                new ClusterResourceChecker(new CpuMemoryOverloadPolicy(new ClusterOverloadConfig()), new ClusterOverloadConfig(), new InMemoryNodeManager()));

        // Simulate being above the threshold by incrementing running queries counter
        manager.incrementRunningQueries();
        manager.incrementRunningQueries();

        // With 2 running queries (at threshold), pacing should be applied
        // First admission should succeed and set the lastAdmittedQueryNanos timestamp
        assertTrue(manager.tryAcquireAdmissionSlot());

        // Immediate second attempt should be denied (need to wait 1 second)
        assertFalse(manager.tryAcquireAdmissionSlot());

        // Verify metrics are tracked when pacing is applied
        assertEquals(manager.getTotalAdmissionAttempts(), 2);
        assertEquals(manager.getTotalAdmissionsGranted(), 1);
        assertEquals(manager.getTotalAdmissionsDenied(), 1);

        // Now simulate queries finishing so that we drop below the threshold
        manager.decrementRunningQueries();
        manager.decrementRunningQueries();

        // With 0 running queries (below threshold of 2), pacing should be bypassed
        // Multiple rapid admissions should all succeed without waiting
        assertTrue(manager.tryAcquireAdmissionSlot());
        assertTrue(manager.tryAcquireAdmissionSlot());
        assertTrue(manager.tryAcquireAdmissionSlot());

        // Verify metrics did NOT increase when pacing was bypassed
        // (should still be the same as before the decrement)
        assertEquals(manager.getTotalAdmissionAttempts(), 2);
        assertEquals(manager.getTotalAdmissionsGranted(), 1);
        assertEquals(manager.getTotalAdmissionsDenied(), 1);
    }
}
