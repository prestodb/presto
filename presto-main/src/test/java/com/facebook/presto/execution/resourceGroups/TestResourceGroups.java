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

import com.facebook.presto.execution.MockQueryExecution;
import com.facebook.presto.execution.MockQueryExecution.MockQueryExecutionBuilder;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroup.RootInternalResourceGroup;
import com.facebook.presto.server.QueryStateInfo;
import com.facebook.presto.server.ResourceGroupInfo;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_QUEUE;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_RUN;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.FAIR;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.QUERY_PRIORITY;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED_FAIR;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.testing.Assertions.assertBetweenInclusive;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Collections.reverse;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestResourceGroups
{
    @Test(timeOut = 10_000)
    public void testQueueFull()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(1);
        root.setHardConcurrencyLimit(1);
        MockQueryExecution query1 = new MockQueryExecutionBuilder().build();
        root.run(query1);
        assertEquals(query1.getState(), RUNNING);
        MockQueryExecution query2 = new MockQueryExecutionBuilder().build();
        root.run(query2);
        assertEquals(query2.getState(), QUEUED);
        MockQueryExecution query3 = new MockQueryExecutionBuilder().build();
        root.run(query3);
        assertEquals(query3.getState(), FAILED);
        assertEquals(query3.getThrowable().getMessage(), "Too many queued queries for \"root\"");
    }

    @Test(timeOut = 10_000)
    public void testFairEligibility()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(4);
        group1.setHardConcurrencyLimit(1);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(4);
        group2.setHardConcurrencyLimit(1);
        InternalResourceGroup group3 = root.getOrCreateSubGroup("3");
        group3.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group3.setMaxQueuedQueries(4);
        group3.setHardConcurrencyLimit(1);
        MockQueryExecution query1a = new MockQueryExecutionBuilder().build();
        group1.run(query1a);
        assertEquals(query1a.getState(), RUNNING);
        MockQueryExecution query1b = new MockQueryExecutionBuilder().build();
        group1.run(query1b);
        assertEquals(query1b.getState(), QUEUED);
        MockQueryExecution query2a = new MockQueryExecutionBuilder().build();
        group2.run(query2a);
        assertEquals(query2a.getState(), QUEUED);
        MockQueryExecution query2b = new MockQueryExecutionBuilder().build();
        group2.run(query2b);
        assertEquals(query2b.getState(), QUEUED);
        MockQueryExecution query3a = new MockQueryExecutionBuilder().build();
        group3.run(query3a);
        assertEquals(query3a.getState(), QUEUED);

        query1a.complete();
        root.updateGroupsAndProcessQueuedQueries();
        // 2a and not 1b should have started, as group1 was not eligible to start a second query
        assertEquals(query1b.getState(), QUEUED);
        assertEquals(query2a.getState(), RUNNING);
        assertEquals(query2b.getState(), QUEUED);
        assertEquals(query3a.getState(), QUEUED);

        query2a.complete();
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query3a.getState(), RUNNING);
        assertEquals(query2b.getState(), QUEUED);
        assertEquals(query1b.getState(), QUEUED);

        query3a.complete();
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query1b.getState(), RUNNING);
        assertEquals(query2b.getState(), QUEUED);
    }

    @Test
    public void testSetSchedulingPolicy()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(4);
        group1.setHardConcurrencyLimit(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(4);
        group2.setHardConcurrencyLimit(2);
        MockQueryExecution query1a = new MockQueryExecutionBuilder().build();
        group1.run(query1a);
        assertEquals(query1a.getState(), RUNNING);
        MockQueryExecution query1b = new MockQueryExecutionBuilder().build();
        group1.run(query1b);
        assertEquals(query1b.getState(), QUEUED);
        MockQueryExecution query1c = new MockQueryExecutionBuilder().build();
        group1.run(query1c);
        assertEquals(query1c.getState(), QUEUED);
        MockQueryExecution query2a = new MockQueryExecutionBuilder().build();
        group2.run(query2a);
        assertEquals(query2a.getState(), QUEUED);

        assertEquals(root.getInfo().getNumEligibleSubGroups(), 2);
        assertEquals(root.getOrCreateSubGroup("1").getQueuedQueries(), 2);
        assertEquals(root.getOrCreateSubGroup("2").getQueuedQueries(), 1);
        assertEquals(root.getSchedulingPolicy(), FAIR);
        root.setSchedulingPolicy(QUERY_PRIORITY);
        assertEquals(root.getInfo().getNumEligibleSubGroups(), 2);
        assertEquals(root.getOrCreateSubGroup("1").getQueuedQueries(), 2);
        assertEquals(root.getOrCreateSubGroup("2").getQueuedQueries(), 1);

        assertEquals(root.getSchedulingPolicy(), QUERY_PRIORITY);
        assertEquals(root.getOrCreateSubGroup("1").getSchedulingPolicy(), QUERY_PRIORITY);
        assertEquals(root.getOrCreateSubGroup("2").getSchedulingPolicy(), QUERY_PRIORITY);
    }

    @Test(timeOut = 10_000)
    public void testFairQueuing()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(4);
        group1.setHardConcurrencyLimit(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(4);
        group2.setHardConcurrencyLimit(2);
        MockQueryExecution query1a = new MockQueryExecutionBuilder().build();
        group1.run(query1a);
        assertEquals(query1a.getState(), RUNNING);
        MockQueryExecution query1b = new MockQueryExecutionBuilder().build();
        group1.run(query1b);
        assertEquals(query1b.getState(), QUEUED);
        MockQueryExecution query1c = new MockQueryExecutionBuilder().build();
        group1.run(query1c);
        assertEquals(query1c.getState(), QUEUED);
        MockQueryExecution query2a = new MockQueryExecutionBuilder().build();
        group2.run(query2a);
        assertEquals(query2a.getState(), QUEUED);

        query1a.complete();
        root.updateGroupsAndProcessQueuedQueries();
        // 1b and not 2a should have started, as it became queued first and group1 was eligible to run more
        assertEquals(query1b.getState(), RUNNING);
        assertEquals(query1c.getState(), QUEUED);
        assertEquals(query2a.getState(), QUEUED);

        // 2a and not 1c should have started, as all eligible sub groups get fair sharing
        query1b.complete();
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query2a.getState(), RUNNING);
        assertEquals(query1c.getState(), QUEUED);
    }

    @Test(timeOut = 10_000)
    public void testMemoryLimit()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, BYTE));
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(3);
        MockQueryExecution query1 = new MockQueryExecutionBuilder().withInitialMemoryUsage(new DataSize(2, BYTE)).build();
        root.run(query1);
        // Process the group to refresh stats
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query1.getState(), RUNNING);
        MockQueryExecution query2 = new MockQueryExecutionBuilder().build();
        root.run(query2);
        assertEquals(query2.getState(), QUEUED);
        MockQueryExecution query3 = new MockQueryExecutionBuilder().build();
        root.run(query3);
        assertEquals(query3.getState(), QUEUED);

        query1.complete();
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
        assertEquals(query3.getState(), RUNNING);
    }

    @Test
    public void testSubgroupMemoryLimit()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(10, BYTE));
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(3);
        InternalResourceGroup subgroup = root.getOrCreateSubGroup("subgroup");
        subgroup.setSoftMemoryLimit(new DataSize(1, BYTE));
        subgroup.setMaxQueuedQueries(4);
        subgroup.setHardConcurrencyLimit(3);

        MockQueryExecution query1 = new MockQueryExecutionBuilder().withInitialMemoryUsage(new DataSize(2, BYTE)).build();
        subgroup.run(query1);
        // Process the group to refresh stats
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query1.getState(), RUNNING);
        MockQueryExecution query2 = new MockQueryExecutionBuilder().build();
        subgroup.run(query2);
        assertEquals(query2.getState(), QUEUED);
        MockQueryExecution query3 = new MockQueryExecutionBuilder().build();
        subgroup.run(query3);
        assertEquals(query3.getState(), QUEUED);

        query1.complete();
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
        assertEquals(query3.getState(), RUNNING);
    }

    @Test(timeOut = 10_000)
    public void testSoftCpuLimit()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, BYTE));
        root.setSoftCpuLimit(new Duration(1, SECONDS));
        root.setHardCpuLimit(new Duration(2, SECONDS));
        root.setCpuQuotaGenerationMillisPerSecond(2000);
        root.setMaxQueuedQueries(1);
        root.setHardConcurrencyLimit(2);

        MockQueryExecution query1 = new MockQueryExecutionBuilder()
                .withInitialMemoryUsage(new DataSize(1, BYTE))
                .withQueryId("query_id")
                .withInitialCpuUsage(new Duration(1, SECONDS))
                .build();
        root.run(query1);
        assertEquals(query1.getState(), RUNNING);

        MockQueryExecution query2 = new MockQueryExecutionBuilder().build();
        root.run(query2);
        assertEquals(query2.getState(), RUNNING);

        MockQueryExecution query3 = new MockQueryExecutionBuilder().build();
        root.run(query3);
        assertEquals(query3.getState(), QUEUED);

        query1.complete();
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
        assertEquals(query3.getState(), QUEUED);

        root.generateCpuQuota(2);
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
        assertEquals(query3.getState(), RUNNING);
    }

    @Test(timeOut = 10_000)
    public void testHardCpuLimit()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, BYTE));
        root.setHardCpuLimit(new Duration(1, SECONDS));
        root.setCpuQuotaGenerationMillisPerSecond(2000);
        root.setMaxQueuedQueries(1);
        root.setHardConcurrencyLimit(1);

        MockQueryExecution query1 = new MockQueryExecutionBuilder()
                .withInitialMemoryUsage(new DataSize(1, BYTE))
                .withQueryId("query_id")
                .withInitialCpuUsage(new Duration(2, SECONDS))
                .build();

        root.run(query1);
        assertEquals(query1.getState(), RUNNING);
        MockQueryExecution query2 = new MockQueryExecutionBuilder().build();
        root.run(query2);
        assertEquals(query2.getState(), QUEUED);

        query1.complete();
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query2.getState(), QUEUED);

        root.generateCpuQuota(2);
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
    }

    /**
     * Test resource group CPU usage update by manually invoking the CPU quota regeneration and queue processing methods
     * that are invoked periodically by the resource group manager
     */
    @Test(timeOut = 10_000)
    public void testCpuUsageUpdateForRunningQuery()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup child = root.getOrCreateSubGroup("child");

        Stream.of(root, child).forEach(group -> {
            group.setCpuQuotaGenerationMillisPerSecond((new Duration(1, MILLISECONDS)).toMillis());
            group.setHardCpuLimit(new Duration(3, MILLISECONDS));
            group.setSoftCpuLimit(new Duration(3, MILLISECONDS));
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
        });

        MockQueryExecution q1 = new MockQueryExecutionBuilder().build();
        child.run(q1);
        assertEquals(q1.getState(), RUNNING);
        q1.consumeCpuTime(new Duration(4, MILLISECONDS));

        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertExceedsCpuLimit(group, 4));

        // q2 gets queued, because the cached usage is greater than the limit
        MockQueryExecution q2 = new MockQueryExecutionBuilder().build();
        child.run(q2);
        assertEquals(q2.getState(), QUEUED);

        // Generating CPU quota before the query finishes. This assertion verifies CPU update during quota generation.
        root.generateCpuQuota(2);
        Stream.of(root, child).forEach(group -> assertWithinCpuLimit(group, 2));

        // An incoming query starts running right away.
        MockQueryExecution q3 = new MockQueryExecutionBuilder().build();
        child.run(q3);
        assertEquals(q3.getState(), RUNNING);

        // A queued query starts running only after invoking `updateGroupsAndProcessQueuedQueries`.
        assertEquals(q2.getState(), QUEUED);
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(q2.getState(), RUNNING);
    }

    @Test(timeOut = 10_000)
    public void testCpuUsageUpdateAtQueryCompletion()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup child = root.getOrCreateSubGroup("child");

        Stream.of(root, child).forEach(group -> {
            group.setCpuQuotaGenerationMillisPerSecond((new Duration(1, MILLISECONDS)).toMillis());
            group.setHardCpuLimit(new Duration(3, MILLISECONDS));
            group.setSoftCpuLimit(new Duration(3, MILLISECONDS));
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
        });

        MockQueryExecution q1 = new MockQueryExecutionBuilder().build();
        child.run(q1);
        assertEquals(q1.getState(), RUNNING);

        q1.consumeCpuTime(new Duration(4, MILLISECONDS));
        q1.complete();

        // Query completion updates the cached usage to 2s. q1 will be removed from runningQueries at this point.
        // Therefore, updateGroupsAndProcessQueuedQueries invocation will not be able to update its CPU usage later.
        Stream.of(root, child).forEach(group -> assertExceedsCpuLimit(group, 4));

        // q2 gets queued since cached usage exceeds the limit.
        MockQueryExecution q2 = new MockQueryExecutionBuilder().build();
        child.run(q2);
        assertEquals(q2.getState(), QUEUED);

        root.generateCpuQuota(2);
        Stream.of(root, child).forEach(group -> assertWithinCpuLimit(group, 2));
        assertEquals(q2.getState(), QUEUED);

        // q2 should run after groups are updated. CPU usage should not be double counted.
        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertWithinCpuLimit(group, 2));
        assertEquals(q2.getState(), RUNNING);
    }

    @Test(timeOut = 10_000)
    public void testMemoryUsageUpdateForRunningQuery()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup child = root.getOrCreateSubGroup("child");

        Stream.of(root, child).forEach(group -> {
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
            group.setSoftMemoryLimit(new DataSize(3, BYTE));
        });

        MockQueryExecution q1 = new MockQueryExecutionBuilder().build();
        child.run(q1);
        assertEquals(q1.getState(), RUNNING);
        q1.setMemoryUsage(new DataSize(4, BYTE));

        Stream.of(root, child).forEach(group -> assertWithinMemoryLimit(group, 0));
        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertExceedsMemoryLimit(group, 4));

        // A new query gets queued since the current usage exceeds the limit.
        MockQueryExecution q2 = new MockQueryExecutionBuilder().build();
        child.run(q2);
        assertEquals(q2.getState(), QUEUED);

        q1.setMemoryUsage(new DataSize(2, BYTE));

        // A new incoming query q3 gets queued since cached usage still exceeds the limit.
        MockQueryExecution q3 = new MockQueryExecutionBuilder().build();
        child.run(q3);
        assertEquals(q3.getState(), QUEUED);

        // q2 and q3 start running when cached usage is updated and queued queries are processed.
        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertWithinMemoryLimit(group, 2));
        assertEquals(q2.getState(), RUNNING);
        assertEquals(q3.getState(), RUNNING);
    }

    @Test(timeOut = 10_000)
    public void testMemoryUsageUpdateAtQueryCompletion()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup child = root.getOrCreateSubGroup("child");

        Stream.of(root, child).forEach(group -> {
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
            group.setSoftMemoryLimit(new DataSize(3, BYTE));
        });

        MockQueryExecution q1 = new MockQueryExecutionBuilder().build();
        child.run(q1);
        assertEquals(q1.getState(), RUNNING);
        q1.setMemoryUsage(new DataSize(4, BYTE));

        Stream.of(root, child).forEach(group -> assertWithinMemoryLimit(group, 0));
        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertExceedsMemoryLimit(group, 4));

        // Query completion should reduce the cached memory usage to 0B.
        q1.complete();
        Stream.of(root, child).forEach(group -> assertWithinMemoryLimit(group, 0));

        // q2 starts running since usage is within the limit.
        MockQueryExecution q2 = new MockQueryExecutionBuilder().build();
        child.run(q2);
        assertEquals(q2.getState(), RUNNING);
    }

    /**
     * A test for correct CPU usage update aggregation and propagation in non-leaf nodes. It uses in a multi
     * level resource group tree, with non-leaf resource groups having more than one child.
     */
    @Test(timeOut = 10_000)
    public void testRecursiveCpuUsageUpdate()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup rootChild1 = root.getOrCreateSubGroup("rootChild1");
        InternalResourceGroup rootChild2 = root.getOrCreateSubGroup("rootChild2");
        InternalResourceGroup rootChild1Child1 = rootChild1.getOrCreateSubGroup("rootChild1Child1");
        InternalResourceGroup rootChild1Child2 = rootChild1.getOrCreateSubGroup("rootChild1Child2");

        // Set the same values in all the groups for some configurations
        Stream.of(root, rootChild1, rootChild2, rootChild1Child1, rootChild1Child2).forEach(group -> {
            group.setCpuQuotaGenerationMillisPerSecond((new Duration(1, MILLISECONDS)).toMillis());
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
        });

        root.setHardCpuLimit(new Duration(16, MILLISECONDS));
        rootChild1.setHardCpuLimit(new Duration(6, MILLISECONDS));

        // Setting a higher limit for leaf nodes to make sure they are always in the limit
        rootChild1Child1.setHardCpuLimit(new Duration(100, MILLISECONDS));
        rootChild1Child2.setHardCpuLimit(new Duration(100, MILLISECONDS));
        rootChild2.setHardCpuLimit(new Duration(100, MILLISECONDS));

        MockQueryExecution q1 = new MockQueryExecutionBuilder().build();
        MockQueryExecution q2 = new MockQueryExecutionBuilder().build();
        MockQueryExecution q3 = new MockQueryExecutionBuilder().build();

        rootChild1Child1.run(q1);
        rootChild1Child2.run(q2);
        rootChild2.run(q3);

        assertEquals(q1.getState(), RUNNING);
        assertEquals(q2.getState(), RUNNING);
        assertEquals(q3.getState(), RUNNING);

        q1.consumeCpuTime(new Duration(4, MILLISECONDS));
        q2.consumeCpuTime(new Duration(10, MILLISECONDS));
        q3.consumeCpuTime(new Duration(4, MILLISECONDS));

        // This invocation will update the cached usage for the nodes
        root.updateGroupsAndProcessQueuedQueries();

        assertExceedsCpuLimit(root, 18);
        assertExceedsCpuLimit(rootChild1, 14);
        assertWithinCpuLimit(rootChild2, 4);
        assertWithinCpuLimit(rootChild1Child1, 4);
        assertWithinCpuLimit(rootChild1Child2, 10);

        // q4 submitted in rootChild2 gets queued because root's CPU usage exceeds the limit
        MockQueryExecution q4 = new MockQueryExecutionBuilder().build();
        rootChild2.run(q4);
        assertEquals(q4.getState(), QUEUED);

        // q5 submitted in rootChild1Child1 gets queued because root's CPU usage exceeds the limit
        MockQueryExecution q5 = new MockQueryExecutionBuilder().build();
        rootChild1Child1.run(q5);
        assertEquals(q5.getState(), QUEUED);

        // Assert CPU usage update after quota regeneration
        root.generateCpuQuota(4);
        assertWithinCpuLimit(root, 14);
        assertExceedsCpuLimit(rootChild1, 10);
        assertWithinCpuLimit(rootChild2, 0);
        assertWithinCpuLimit(rootChild1Child1, 0);
        assertWithinCpuLimit(rootChild1Child2, 6);

        root.updateGroupsAndProcessQueuedQueries();

        // q4 gets dequeued, because CPU usages of root and rootChild2 are below their limits.
        assertEquals(q4.getState(), RUNNING);
        // q5 does not get dequeued, because rootChild1's CPU usage exceeds the limit.
        assertEquals(q5.getState(), QUEUED);

        q2.consumeCpuTime(new Duration(3, MILLISECONDS));
        q2.complete();

        // Query completion updates cached CPU usage of root, rootChild1 and rootChild1Child2.
        assertExceedsCpuLimit(root, 17);
        assertExceedsCpuLimit(rootChild1, 13);
        assertWithinCpuLimit(rootChild1Child2, 9);

        // q6 in rootChild2 gets queued because root's CPU usage exceeds the limit.
        MockQueryExecution q6 = new MockQueryExecutionBuilder().build();
        rootChild2.run(q6);
        assertEquals(q6.getState(), QUEUED);

        // Assert usage after regeneration
        root.generateCpuQuota(6);
        assertWithinCpuLimit(root, 11);
        assertExceedsCpuLimit(rootChild1, 7);
        assertWithinCpuLimit(rootChild2, 0);
        assertWithinCpuLimit(rootChild1Child1, 0);
        assertWithinCpuLimit(rootChild1Child2, 3);

        root.updateGroupsAndProcessQueuedQueries();

        // q5 is queued, because rootChild1's usage still exceeds the limit.
        assertEquals(q5.getState(), QUEUED);
        // q6 starts running, because usage in rootChild2 and root are within their limits.
        assertEquals(q6.getState(), RUNNING);

        // q5 starts running after rootChild1's usage comes within the limit
        root.generateCpuQuota(2);
        assertWithinCpuLimit(rootChild1, 5);
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(q5.getState(), RUNNING);
    }

    /**
     * A test for correct memory usage update aggregation and propagation in non-leaf nodes. It uses in a multi
     * level resource group tree, with non-leaf resource groups having more than one child.
     */
    @Test(timeOut = 10_000)
    public void testMemoryUpdateRecursively()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup rootChild1 = root.getOrCreateSubGroup("rootChild1");
        InternalResourceGroup rootChild2 = root.getOrCreateSubGroup("rootChild2");
        InternalResourceGroup rootChild1Child1 = rootChild1.getOrCreateSubGroup("rootChild1Child1");
        InternalResourceGroup rootChild1Child2 = rootChild1.getOrCreateSubGroup("rootChild1Child2");

        // Set the same values in all the groups for some configurations
        Stream.of(root, rootChild1, rootChild2, rootChild1Child1, rootChild1Child2).forEach(group -> {
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
        });

        root.setSoftMemoryLimit(new DataSize(8, BYTE));
        rootChild1.setSoftMemoryLimit(new DataSize(3, BYTE));
        // Setting a higher limit for leaf nodes
        rootChild2.setSoftMemoryLimit(new DataSize(100, BYTE));
        rootChild1Child1.setSoftMemoryLimit(new DataSize(100, BYTE));
        rootChild1Child2.setSoftMemoryLimit(new DataSize(100, BYTE));

        MockQueryExecution q1 = new MockQueryExecutionBuilder().build();
        MockQueryExecution q2 = new MockQueryExecutionBuilder().build();
        MockQueryExecution q3 = new MockQueryExecutionBuilder().build();

        rootChild1Child1.run(q1);
        rootChild1Child2.run(q2);
        rootChild2.run(q3);

        assertEquals(q1.getState(), RUNNING);
        assertEquals(q2.getState(), RUNNING);
        assertEquals(q3.getState(), RUNNING);

        q1.setMemoryUsage(new DataSize(2, BYTE));
        q2.setMemoryUsage(new DataSize(5, BYTE));
        q3.setMemoryUsage(new DataSize(2, BYTE));

        // The cached memory usage gets updated for the tree
        root.updateGroupsAndProcessQueuedQueries();
        assertExceedsMemoryLimit(root, 9);
        assertExceedsMemoryLimit(rootChild1, 7);
        assertWithinMemoryLimit(rootChild2, 2);
        assertWithinMemoryLimit(rootChild1Child1, 2);
        assertWithinMemoryLimit(rootChild1Child2, 5);

        // q4 submitted in rootChild2 gets queued because root's memory usage exceeds the limit
        MockQueryExecution q4 = new MockQueryExecutionBuilder().build();
        rootChild2.run(q4);
        assertEquals(q4.getState(), QUEUED);

        // q5 submitted in rootChild1Child1 gets queued because root's memory usage) exceeds the limit
        MockQueryExecution q5 = new MockQueryExecutionBuilder().build();
        rootChild1Child1.run(q5);
        assertEquals(q5.getState(), QUEUED);

        q1.setMemoryUsage(new DataSize(0, BYTE));

        root.updateGroupsAndProcessQueuedQueries();
        assertWithinMemoryLimit(root, 7);
        assertExceedsMemoryLimit(rootChild1, 5);
        assertWithinMemoryLimit(rootChild1Child1, 0);

        // q4 starts running since usage in root and rootChild2 is within the limits
        assertEquals(q4.getState(), RUNNING);
        // q5 is queued since usage in rootChild1 exceeds the limit.
        assertEquals(q5.getState(), QUEUED);

        // q2's completion triggers memory updates
        q2.complete();
        assertWithinMemoryLimit(root, 2);
        assertWithinMemoryLimit(rootChild1, 0);

        // An incoming query starts running
        MockQueryExecution q6 = new MockQueryExecutionBuilder().build();
        rootChild1Child2.run(q6);
        assertEquals(q6.getState(), RUNNING);

        // queued queries will start running after the update
        assertEquals(q5.getState(), QUEUED);
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(q5.getState(), RUNNING);
    }

    @Test(timeOut = 10_000)
    public void testPriorityScheduling()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(100);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(QUERY_PRIORITY);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(100);
        group1.setHardConcurrencyLimit(1);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(100);
        group2.setHardConcurrencyLimit(1);

        SortedMap<Integer, MockQueryExecution> queries = new TreeMap<>();

        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int priority;
            do {
                priority = random.nextInt(1_000_000) + 1;
            }
            while (queries.containsKey(priority));

            MockQueryExecution query = new MockQueryExecutionBuilder()
                    .withQueryId("query_id")
                    .withPriority(priority)
                    .build();

            if (random.nextBoolean()) {
                group1.run(query);
            }
            else {
                group2.run(query);
            }
            queries.put(priority, query);
        }

        root.setHardConcurrencyLimit(1);

        List<MockQueryExecution> orderedQueries = new ArrayList<>(queries.values());
        reverse(orderedQueries);

        for (MockQueryExecution query : orderedQueries) {
            root.updateGroupsAndProcessQueuedQueries();
            assertEquals(query.getState(), RUNNING);
            query.complete();
        }
    }

    @Test(timeOut = 10_000)
    public void testWeightedScheduling()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(2);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(2);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(2);

        Set<MockQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 2);
        Set<MockQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 2);
        root.setHardConcurrencyLimit(1);

        int group2Ran = 0;
        for (int i = 0; i < 1000; i++) {
            for (Iterator<MockQueryExecution> iterator = group1Queries.iterator(); iterator.hasNext(); ) {
                MockQueryExecution query = iterator.next();
                if (query.getState() == RUNNING) {
                    query.complete();
                    iterator.remove();
                }
            }
            group2Ran += completeGroupQueries(group2Queries);
            root.updateGroupsAndProcessQueuedQueries();
            group1Queries = fillGroupTo(group1, group1Queries, 2);
            group2Queries = fillGroupTo(group2, group2Queries, 2);
        }

        // group1 has a weight of 1 and group2 has a weight of 2, so group2 should account for (2 / (1 + 2)) of the queries.
        // since this is stochastic, we check that the result of 1000 trials are 2/3 with 99.9999% confidence
        BinomialDistribution binomial = new BinomialDistribution(1000, 2.0 / 3.0);
        int lowerBound = binomial.inverseCumulativeProbability(0.000001);
        int upperBound = binomial.inverseCumulativeProbability(0.999999);
        assertLessThan(group2Ran, upperBound);
        assertGreaterThan(group2Ran, lowerBound);
    }

    @Test(timeOut = 10_000)
    public void testWeightedFairScheduling()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(50);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED_FAIR);

        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(50);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        group1.setSchedulingWeight(1);

        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(50);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(2);

        Set<MockQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 4);
        Set<MockQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 4);
        root.setHardConcurrencyLimit(3);

        int group1Ran = 0;
        int group2Ran = 0;
        for (int i = 0; i < 1000; i++) {
            group1Ran += completeGroupQueries(group1Queries);
            group2Ran += completeGroupQueries(group2Queries);
            root.updateGroupsAndProcessQueuedQueries();
            group1Queries = fillGroupTo(group1, group1Queries, 4);
            group2Queries = fillGroupTo(group2, group2Queries, 4);
        }

        // group1 has a weight of 1 and group2 has a weight of 2, so group2 should account for (2 / (1 + 2)) * 3000 queries.
        assertBetweenInclusive(group1Ran, 995, 1000);
        assertBetweenInclusive(group2Ran, 1995, 2000);
    }

    @Test(timeOut = 10_000)
    public void testWeightedFairSchedulingEqualWeights()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(50);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED_FAIR);

        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(50);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        group1.setSchedulingWeight(1);

        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(50);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(1);

        InternalResourceGroup group3 = root.getOrCreateSubGroup("3");
        group3.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group3.setMaxQueuedQueries(50);
        group3.setHardConcurrencyLimit(2);
        group3.setSoftConcurrencyLimit(2);
        group3.setSchedulingWeight(2);

        Set<MockQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 4);
        Set<MockQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 4);
        Set<MockQueryExecution> group3Queries = fillGroupTo(group3, ImmutableSet.of(), 4);
        root.setHardConcurrencyLimit(4);

        int group1Ran = 0;
        int group2Ran = 0;
        int group3Ran = 0;
        for (int i = 0; i < 1000; i++) {
            group1Ran += completeGroupQueries(group1Queries);
            group2Ran += completeGroupQueries(group2Queries);
            group3Ran += completeGroupQueries(group3Queries);
            root.updateGroupsAndProcessQueuedQueries();
            group1Queries = fillGroupTo(group1, group1Queries, 4);
            group2Queries = fillGroupTo(group2, group2Queries, 4);
            group3Queries = fillGroupTo(group3, group3Queries, 4);
        }

        // group 3 should run approximately 2x the number of queries of 1 and 2
        BinomialDistribution binomial = new BinomialDistribution(4000, 1.0 / 4.0);
        int lowerBound = binomial.inverseCumulativeProbability(0.000001);
        int upperBound = binomial.inverseCumulativeProbability(0.999999);

        assertBetweenInclusive(group1Ran, lowerBound, upperBound);
        assertBetweenInclusive(group2Ran, lowerBound, upperBound);
        assertBetweenInclusive(group3Ran, 2 * lowerBound, 2 * upperBound);
    }

    @Test(timeOut = 10_000)
    public void testWeightedFairSchedulingNoStarvation()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(50);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED_FAIR);

        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(50);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        group1.setSchedulingWeight(1);

        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(50);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(2);

        Set<MockQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 4);
        Set<MockQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 4);
        root.setHardConcurrencyLimit(1);

        int group1Ran = 0;
        for (int i = 0; i < 2000; i++) {
            group1Ran += completeGroupQueries(group1Queries);
            completeGroupQueries(group2Queries);
            root.updateGroupsAndProcessQueuedQueries();
            group1Queries = fillGroupTo(group1, group1Queries, 4);
            group2Queries = fillGroupTo(group2, group2Queries, 4);
        }

        assertEquals(group1Ran, 1000);
        assertEquals(group1Ran, 1000);
    }

    @Test
    public void testGetInfo()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(40);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        rootA.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(2);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b");
        rootB.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(2);
        rootB.setSchedulingWeight(2);
        rootB.setSchedulingPolicy(QUERY_PRIORITY);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
        rootAX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y");
        rootAY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(10);

        InternalResourceGroup rootBX = rootB.getOrCreateSubGroup("x");
        rootBX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootBX.setMaxQueuedQueries(10);
        rootBX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootBY = rootB.getOrCreateSubGroup("y");
        rootBY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootBY.setMaxQueuedQueries(10);
        rootBY.setHardConcurrencyLimit(10);

        // Queue 40 queries (= maxQueuedQueries (40) + maxRunningQueries (0))
        Set<MockQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 10, false);
        queries.addAll(fillGroupTo(rootAY, ImmutableSet.of(), 10, false));
        queries.addAll(fillGroupTo(rootBX, ImmutableSet.of(), 10, true));
        queries.addAll(fillGroupTo(rootBY, ImmutableSet.of(), 10, true));

        ResourceGroupInfo info = root.getInfo();
        assertEquals(info.getNumRunningQueries(), 0);
        assertEquals(info.getNumQueuedQueries(), 40);

        // root.maxRunningQueries = 4, root.a.maxRunningQueries = 2, root.b.maxRunningQueries = 2. Will have 4 queries running and 36 left queued.
        root.setHardConcurrencyLimit(4);
        root.updateGroupsAndProcessQueuedQueries();
        info = root.getInfo();
        assertEquals(info.getNumRunningQueries(), 4);
        assertEquals(info.getNumQueuedQueries(), 36);

        // Complete running queries
        Iterator<MockQueryExecution> iterator = queries.iterator();
        while (iterator.hasNext()) {
            MockQueryExecution query = iterator.next();
            if (query.getState() == RUNNING) {
                query.complete();
                iterator.remove();
            }
        }

        // 4 more queries start running, 32 left queued.
        root.updateGroupsAndProcessQueuedQueries();
        info = root.getInfo();
        assertEquals(info.getNumRunningQueries(), 4);
        assertEquals(info.getNumQueuedQueries(), 32);

        // root.maxRunningQueries = 10, root.a.maxRunningQueries = 2, root.b.maxRunningQueries = 2. Still only have 4 running queries and 32 left queued.
        root.setHardConcurrencyLimit(10);
        root.updateGroupsAndProcessQueuedQueries();
        info = root.getInfo();
        assertEquals(info.getNumRunningQueries(), 4);
        assertEquals(info.getNumQueuedQueries(), 32);

        // root.maxRunningQueries = 10, root.a.maxRunningQueries = 2, root.b.maxRunningQueries = 10. Will have 10 running queries and 26 left queued.
        rootB.setHardConcurrencyLimit(10);
        root.updateGroupsAndProcessQueuedQueries();
        info = root.getInfo();
        assertEquals(info.getNumRunningQueries(), 10);
        assertEquals(info.getNumQueuedQueries(), 26);
    }

    @Test
    public void testGetResourceGroupStateInfo()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, GIGABYTE));
        root.setMaxQueuedQueries(40);
        root.setHardConcurrencyLimit(10);
        root.setSchedulingPolicy(WEIGHTED);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        rootA.setSoftMemoryLimit(new DataSize(10, MEGABYTE));
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(0);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b");
        rootB.setSoftMemoryLimit(new DataSize(5, MEGABYTE));
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(1);
        rootB.setSchedulingWeight(2);
        rootB.setSchedulingPolicy(QUERY_PRIORITY);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
        rootAX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y");
        rootAY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(10);

        Set<MockQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 5, false);
        queries.addAll(fillGroupTo(rootAY, ImmutableSet.of(), 5, false));
        queries.addAll(fillGroupTo(rootB, ImmutableSet.of(), 10, true));

        ResourceGroupInfo rootInfo = root.getFullInfo();
        assertEquals(rootInfo.getId(), root.getId());
        assertEquals(rootInfo.getState(), CAN_RUN);
        assertEquals(rootInfo.getSoftMemoryLimit(), root.getSoftMemoryLimit());
        assertEquals(rootInfo.getMemoryUsage(), new DataSize(0, BYTE));
        assertEquals(rootInfo.getCpuUsage(), new Duration(0, MILLISECONDS));
        assertEquals(rootInfo.getSubGroups().size(), 2);
        assertGroupInfoEquals(rootInfo.getSubGroups().get(0), rootA.getInfo());
        assertEquals(rootInfo.getSubGroups().get(0).getId(), rootA.getId());
        assertEquals(rootInfo.getSubGroups().get(0).getState(), CAN_QUEUE);
        assertEquals(rootInfo.getSubGroups().get(0).getSoftMemoryLimit(), rootA.getSoftMemoryLimit());
        assertEquals(rootInfo.getSubGroups().get(0).getHardConcurrencyLimit(), rootA.getHardConcurrencyLimit());
        assertEquals(rootInfo.getSubGroups().get(0).getMaxQueuedQueries(), rootA.getMaxQueuedQueries());
        assertEquals(rootInfo.getSubGroups().get(0).getNumEligibleSubGroups(), 2);
        assertEquals(rootInfo.getSubGroups().get(0).getNumRunningQueries(), 0);
        assertEquals(rootInfo.getSubGroups().get(0).getNumQueuedQueries(), 10);
        assertGroupInfoEquals(rootInfo.getSubGroups().get(1), rootB.getInfo());
        assertEquals(rootInfo.getSubGroups().get(1).getId(), rootB.getId());
        assertEquals(rootInfo.getSubGroups().get(1).getState(), CAN_QUEUE);
        assertEquals(rootInfo.getSubGroups().get(1).getSoftMemoryLimit(), rootB.getSoftMemoryLimit());
        assertEquals(rootInfo.getSubGroups().get(1).getHardConcurrencyLimit(), rootB.getHardConcurrencyLimit());
        assertEquals(rootInfo.getSubGroups().get(1).getMaxQueuedQueries(), rootB.getMaxQueuedQueries());
        assertEquals(rootInfo.getSubGroups().get(1).getNumEligibleSubGroups(), 0);
        assertEquals(rootInfo.getSubGroups().get(1).getNumRunningQueries(), 1);
        assertEquals(rootInfo.getSubGroups().get(1).getNumQueuedQueries(), 9);
        assertEquals(rootInfo.getSoftConcurrencyLimit(), root.getSoftConcurrencyLimit());
        assertEquals(rootInfo.getHardConcurrencyLimit(), root.getHardConcurrencyLimit());
        assertEquals(rootInfo.getMaxQueuedQueries(), root.getMaxQueuedQueries());
        assertEquals(rootInfo.getNumQueuedQueries(), 19);
        assertEquals(rootInfo.getRunningQueries().size(), 1);
        QueryStateInfo queryInfo = rootInfo.getRunningQueries().get(0);
        assertEquals(queryInfo.getResourceGroupId(), Optional.of(rootB.getId()));
    }

    @Test
    public void testGetBlockedQueuedQueries()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(40);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        rootA.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(8);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
        rootAX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(8);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y");
        rootAY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(5);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b");
        rootB.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(8);

        InternalResourceGroup rootBX = rootB.getOrCreateSubGroup("x");
        rootBX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootBX.setMaxQueuedQueries(10);
        rootBX.setHardConcurrencyLimit(8);

        InternalResourceGroup rootBY = rootB.getOrCreateSubGroup("y");
        rootBY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootBY.setMaxQueuedQueries(10);
        rootBY.setHardConcurrencyLimit(5);

        // Queue 40 queries (= maxQueuedQueries (40) + maxRunningQueries (0))
        Set<MockQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 10, false);
        queries.addAll(fillGroupTo(rootAY, ImmutableSet.of(), 10, false));
        queries.addAll(fillGroupTo(rootBX, ImmutableSet.of(), 10, true));
        queries.addAll(fillGroupTo(rootBY, ImmutableSet.of(), 10, true));

        assertEquals(root.getWaitingQueuedQueries(), 16);
        assertEquals(rootA.getWaitingQueuedQueries(), 13);
        assertEquals(rootAX.getWaitingQueuedQueries(), 10);
        assertEquals(rootAY.getWaitingQueuedQueries(), 10);
        assertEquals(rootB.getWaitingQueuedQueries(), 13);
        assertEquals(rootBX.getWaitingQueuedQueries(), 10);
        assertEquals(rootBY.getWaitingQueuedQueries(), 10);

        root.setHardConcurrencyLimit(20);
        root.updateGroupsAndProcessQueuedQueries();
        assertEquals(root.getWaitingQueuedQueries(), 0);
        assertEquals(rootA.getWaitingQueuedQueries(), 5);
        assertEquals(rootAX.getWaitingQueuedQueries(), 6);
        assertEquals(rootAY.getWaitingQueuedQueries(), 6);
        assertEquals(rootB.getWaitingQueuedQueries(), 5);
        assertEquals(rootBX.getWaitingQueuedQueries(), 6);
        assertEquals(rootBY.getWaitingQueuedQueries(), 6);
    }

    private static int completeGroupQueries(Set<MockQueryExecution> groupQueries)
    {
        int groupRan = 0;
        for (Iterator<MockQueryExecution> iterator = groupQueries.iterator(); iterator.hasNext(); ) {
            MockQueryExecution query = iterator.next();
            if (query.getState() == RUNNING) {
                query.complete();
                iterator.remove();
                groupRan++;
            }
        }
        return groupRan;
    }

    private static Set<MockQueryExecution> fillGroupTo(InternalResourceGroup group, Set<MockQueryExecution> existingQueries, int count)
    {
        return fillGroupTo(group, existingQueries, count, false);
    }

    private static Set<MockQueryExecution> fillGroupTo(InternalResourceGroup group, Set<MockQueryExecution> existingQueries, int count, boolean queryPriority)
    {
        int existingCount = existingQueries.size();
        Set<MockQueryExecution> queries = new HashSet<>(existingQueries);
        for (int i = 0; i < count - existingCount; i++) {
            MockQueryExecution query = new MockQueryExecutionBuilder()
                    .withQueryId(group.getId().toString().replace(".", "") + i)
                    .withPriority(queryPriority ? i + 1 : 1)
                    .build();
            queries.add(query);
            group.run(query);
        }
        return queries;
    }

    private static void assertGroupInfoEquals(ResourceGroupInfo actual, ResourceGroupInfo expected)
    {
        assertTrue(actual.getSchedulingWeight() == expected.getSchedulingWeight() &&
                actual.getSoftConcurrencyLimit() == expected.getSoftConcurrencyLimit() &&
                actual.getHardConcurrencyLimit() == expected.getHardConcurrencyLimit() &&
                actual.getMaxQueuedQueries() == expected.getMaxQueuedQueries() &&
                actual.getNumQueuedQueries() == expected.getNumQueuedQueries() &&
                actual.getNumRunningQueries() == expected.getNumRunningQueries() &&
                actual.getNumEligibleSubGroups() == expected.getNumEligibleSubGroups() &&
                Objects.equals(actual.getId(), expected.getId()) &&
                actual.getState() == expected.getState() &&
                actual.getSchedulingPolicy() == expected.getSchedulingPolicy() &&
                Objects.equals(actual.getSoftMemoryLimit(), expected.getSoftMemoryLimit()) &&
                Objects.equals(actual.getMemoryUsage(), expected.getMemoryUsage()) &&
                Objects.equals(actual.getCpuUsage(), expected.getCpuUsage()));
    }

    private static void assertExceedsCpuLimit(InternalResourceGroup group, long expectedMillis)
    {
        long actualMillis = group.getResourceUsageSnapshot().getCpuUsageMillis();
        assertEquals(actualMillis, expectedMillis);
        assertTrue(actualMillis >= group.getHardCpuLimit().toMillis());
    }

    private static void assertWithinCpuLimit(InternalResourceGroup group, long expectedMillis)
    {
        long actualMillis = group.getResourceUsageSnapshot().getCpuUsageMillis();
        assertEquals(actualMillis, expectedMillis);
        assertTrue(actualMillis < group.getHardCpuLimit().toMillis());
    }

    private static void assertExceedsMemoryLimit(InternalResourceGroup group, long expectedBytes)
    {
        long actualBytes = group.getResourceUsageSnapshot().getMemoryUsageBytes();
        assertEquals(actualBytes, expectedBytes);
        assertTrue(actualBytes > group.getSoftMemoryLimit().toBytes());
    }

    private static void assertWithinMemoryLimit(InternalResourceGroup group, long expectedBytes)
    {
        long actualBytes = group.getResourceUsageSnapshot().getMemoryUsageBytes();
        assertEquals(actualBytes, expectedBytes);
        assertTrue(actualBytes <= group.getSoftMemoryLimit().toBytes());
    }
}
