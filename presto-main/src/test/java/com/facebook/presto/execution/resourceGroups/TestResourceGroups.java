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

import com.facebook.presto.execution.MockManagedQueryExecution;
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

import static com.facebook.airlift.testing.Assertions.assertBetweenInclusive;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.airlift.testing.Assertions.assertLessThan;
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
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Collections.reverse;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestResourceGroups
{
    @Test(timeOut = 10_000)
    public void testQueueFull()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(1);
        root.setHardConcurrencyLimit(1);
        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        query1.startWaitingForPrerequisites();
        root.run(query1);
        assertEquals(query1.getState(), RUNNING);
        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        root.run(query2);
        assertEquals(query2.getState(), QUEUED);
        MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
        query3.startWaitingForPrerequisites();
        root.run(query3);
        assertEquals(query3.getState(), FAILED);
        assertEquals(query3.getThrowable().getMessage(), "Too many queued queries for \"root\"");
    }

    @Test(timeOut = 10_000)
    public void testFairEligibility()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1", true);
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(4);
        group1.setHardConcurrencyLimit(1);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2", true);
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(4);
        group2.setHardConcurrencyLimit(1);
        InternalResourceGroup group3 = root.getOrCreateSubGroup("3", true);
        group3.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group3.setMaxQueuedQueries(4);
        group3.setHardConcurrencyLimit(1);
        MockManagedQueryExecution query1a = new MockManagedQueryExecution(0);
        query1a.startWaitingForPrerequisites();
        group1.run(query1a);
        assertEquals(query1a.getState(), RUNNING);
        MockManagedQueryExecution query1b = new MockManagedQueryExecution(0);
        query1b.startWaitingForPrerequisites();
        group1.run(query1b);
        assertEquals(query1b.getState(), QUEUED);
        MockManagedQueryExecution query2a = new MockManagedQueryExecution(0);
        query2a.startWaitingForPrerequisites();
        group2.run(query2a);
        assertEquals(query2a.getState(), QUEUED);
        MockManagedQueryExecution query2b = new MockManagedQueryExecution(0);
        query2b.startWaitingForPrerequisites();
        group2.run(query2b);
        assertEquals(query2b.getState(), QUEUED);
        MockManagedQueryExecution query3a = new MockManagedQueryExecution(0);
        query3a.startWaitingForPrerequisites();
        group3.run(query3a);
        assertEquals(query3a.getState(), QUEUED);

        query1a.complete();
        root.processQueuedQueries();
        // 2a and not 1b should have started, as group1 was not eligible to start a second query
        assertEquals(query1b.getState(), QUEUED);
        assertEquals(query2a.getState(), RUNNING);
        assertEquals(query2b.getState(), QUEUED);
        assertEquals(query3a.getState(), QUEUED);

        query2a.complete();
        root.processQueuedQueries();
        assertEquals(query3a.getState(), RUNNING);
        assertEquals(query2b.getState(), QUEUED);
        assertEquals(query1b.getState(), QUEUED);

        query3a.complete();
        root.processQueuedQueries();
        assertEquals(query1b.getState(), RUNNING);
        assertEquals(query2b.getState(), QUEUED);
    }

    @Test
    public void testSetSchedulingPolicy()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1", true);
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(4);
        group1.setHardConcurrencyLimit(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2", true);
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(4);
        group2.setHardConcurrencyLimit(2);
        MockManagedQueryExecution query1a = new MockManagedQueryExecution(0);
        query1a.startWaitingForPrerequisites();
        group1.run(query1a);
        assertEquals(query1a.getState(), RUNNING);
        MockManagedQueryExecution query1b = new MockManagedQueryExecution(0);
        query1b.startWaitingForPrerequisites();
        group1.run(query1b);
        assertEquals(query1b.getState(), QUEUED);
        MockManagedQueryExecution query1c = new MockManagedQueryExecution(0);
        query1c.startWaitingForPrerequisites();
        group1.run(query1c);
        assertEquals(query1c.getState(), QUEUED);
        MockManagedQueryExecution query2a = new MockManagedQueryExecution(0);
        query2a.startWaitingForPrerequisites();
        group2.run(query2a);
        assertEquals(query2a.getState(), QUEUED);

        assertEquals(root.getInfo().getNumEligibleSubGroups(), 2);
        assertEquals(root.getOrCreateSubGroup("1", true).getQueuedQueries(), 2);
        assertEquals(root.getOrCreateSubGroup("2", true).getQueuedQueries(), 1);
        assertEquals(root.getSchedulingPolicy(), FAIR);
        root.setSchedulingPolicy(QUERY_PRIORITY);
        assertEquals(root.getInfo().getNumEligibleSubGroups(), 2);
        assertEquals(root.getOrCreateSubGroup("1", true).getQueuedQueries(), 2);
        assertEquals(root.getOrCreateSubGroup("2", true).getQueuedQueries(), 1);

        assertEquals(root.getSchedulingPolicy(), QUERY_PRIORITY);
        assertEquals(root.getOrCreateSubGroup("1", true).getSchedulingPolicy(), QUERY_PRIORITY);
        assertEquals(root.getOrCreateSubGroup("2", true).getSchedulingPolicy(), QUERY_PRIORITY);
    }

    @Test(timeOut = 10_000)
    public void testFairQueuing()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1", true);
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(4);
        group1.setHardConcurrencyLimit(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2", true);
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(4);
        group2.setHardConcurrencyLimit(2);
        MockManagedQueryExecution query1a = new MockManagedQueryExecution(0);
        query1a.startWaitingForPrerequisites();
        group1.run(query1a);
        assertEquals(query1a.getState(), RUNNING);
        MockManagedQueryExecution query1b = new MockManagedQueryExecution(0);
        query1b.startWaitingForPrerequisites();
        group1.run(query1b);
        assertEquals(query1b.getState(), QUEUED);
        MockManagedQueryExecution query1c = new MockManagedQueryExecution(0);
        query1c.startWaitingForPrerequisites();
        group1.run(query1c);
        assertEquals(query1c.getState(), QUEUED);
        MockManagedQueryExecution query2a = new MockManagedQueryExecution(0);
        query2a.startWaitingForPrerequisites();
        group2.run(query2a);
        assertEquals(query2a.getState(), QUEUED);

        query1a.complete();
        root.processQueuedQueries();
        // 1b and not 2a should have started, as it became queued first and group1 was eligible to run more
        assertEquals(query1b.getState(), RUNNING);
        assertEquals(query1c.getState(), QUEUED);
        assertEquals(query2a.getState(), QUEUED);

        // 2a and not 1c should have started, as all eligible sub groups get fair sharing
        query1b.complete();
        root.processQueuedQueries();
        assertEquals(query2a.getState(), RUNNING);
        assertEquals(query1c.getState(), QUEUED);
    }

    @Test(timeOut = 10_000)
    public void testMemoryLimit()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, BYTE));
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(3);
        MockManagedQueryExecution query1 = new MockManagedQueryExecution(2);
        query1.startWaitingForPrerequisites();
        root.run(query1);
        // Process the group to refresh stats
        root.processQueuedQueries();
        assertEquals(query1.getState(), RUNNING);
        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        root.run(query2);
        assertEquals(query2.getState(), QUEUED);
        MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
        query3.startWaitingForPrerequisites();
        root.run(query3);
        assertEquals(query3.getState(), QUEUED);

        query1.complete();
        root.processQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
        assertEquals(query3.getState(), RUNNING);
    }

    @Test
    public void testSubgroupMemoryLimit()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(10, BYTE));
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(3);
        InternalResourceGroup subgroup = root.getOrCreateSubGroup("subgroup", true);
        subgroup.setSoftMemoryLimit(new DataSize(1, BYTE));
        subgroup.setMaxQueuedQueries(4);
        subgroup.setHardConcurrencyLimit(3);

        MockManagedQueryExecution query1 = new MockManagedQueryExecution(2);
        query1.startWaitingForPrerequisites();
        subgroup.run(query1);
        // Process the group to refresh stats
        root.processQueuedQueries();
        assertEquals(query1.getState(), RUNNING);
        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        subgroup.run(query2);
        assertEquals(query2.getState(), QUEUED);
        MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
        query3.startWaitingForPrerequisites();
        subgroup.run(query3);
        assertEquals(query3.getState(), QUEUED);

        query1.complete();
        root.processQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
        assertEquals(query3.getState(), RUNNING);
    }

    @Test(timeOut = 10_000)
    public void testSoftCpuLimit()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, BYTE));
        root.setSoftCpuLimit(new Duration(1, SECONDS));
        root.setHardCpuLimit(new Duration(2, SECONDS));
        root.setCpuQuotaGenerationMillisPerSecond(2000);
        root.setMaxQueuedQueries(1);
        root.setHardConcurrencyLimit(2);

        MockManagedQueryExecution query1 = new MockManagedQueryExecution(1, "query_id", 1, new Duration(1, SECONDS));
        query1.startWaitingForPrerequisites();
        root.run(query1);
        assertEquals(query1.getState(), RUNNING);

        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        root.run(query2);
        assertEquals(query2.getState(), RUNNING);

        MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
        query3.startWaitingForPrerequisites();
        root.run(query3);
        assertEquals(query3.getState(), QUEUED);

        query1.complete();
        root.processQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
        assertEquals(query3.getState(), QUEUED);

        root.generateCpuQuota(2);
        root.processQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
        assertEquals(query3.getState(), RUNNING);
    }

    @Test(timeOut = 10_000)
    public void testHardCpuLimit()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, BYTE));
        root.setHardCpuLimit(new Duration(1, SECONDS));
        root.setCpuQuotaGenerationMillisPerSecond(2000);
        root.setMaxQueuedQueries(1);
        root.setHardConcurrencyLimit(1);
        MockManagedQueryExecution query1 = new MockManagedQueryExecution(1, "query_id", 1, new Duration(2, SECONDS));
        query1.startWaitingForPrerequisites();
        root.run(query1);
        assertEquals(query1.getState(), RUNNING);
        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        root.run(query2);
        assertEquals(query2.getState(), QUEUED);

        query1.complete();
        root.processQueuedQueries();
        assertEquals(query2.getState(), QUEUED);

        root.generateCpuQuota(2);
        root.processQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
    }

    @Test(timeOut = 10_000)
    public void testPriorityScheduling()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(100);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(QUERY_PRIORITY);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1", true);
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(100);
        group1.setHardConcurrencyLimit(1);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2", true);
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(100);
        group2.setHardConcurrencyLimit(1);

        SortedMap<Integer, MockManagedQueryExecution> queries = new TreeMap<>();

        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int priority;
            do {
                priority = random.nextInt(1_000_000) + 1;
            }
            while (queries.containsKey(priority));

            MockManagedQueryExecution query = new MockManagedQueryExecution(0, "query_id", priority);
            if (random.nextBoolean()) {
                group1.run(query);
            }
            else {
                group2.run(query);
            }
            queries.put(priority, query);
        }

        root.setHardConcurrencyLimit(1);

        List<MockManagedQueryExecution> orderedQueries = new ArrayList<>(queries.values());
        reverse(orderedQueries);

        for (MockManagedQueryExecution query : orderedQueries) {
            root.processQueuedQueries();
            assertEquals(query.getState(), RUNNING);
            query.complete();
        }
    }

    @Test(timeOut = 10_000)
    public void testWeightedScheduling()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1", true);
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(2);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2", true);
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(2);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(2);

        Set<MockManagedQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 2);
        Set<MockManagedQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 2);
        root.setHardConcurrencyLimit(1);

        int group2Ran = 0;
        for (int i = 0; i < 1000; i++) {
            for (Iterator<MockManagedQueryExecution> iterator = group1Queries.iterator(); iterator.hasNext(); ) {
                MockManagedQueryExecution query = iterator.next();
                if (query.getState() == RUNNING) {
                    query.complete();
                    iterator.remove();
                }
            }
            group2Ran += completeGroupQueries(group2Queries);
            root.processQueuedQueries();
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
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(50);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED_FAIR);

        InternalResourceGroup group1 = root.getOrCreateSubGroup("1", true);
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(50);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        group1.setSchedulingWeight(1);

        InternalResourceGroup group2 = root.getOrCreateSubGroup("2", true);
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(50);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(2);

        Set<MockManagedQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 4);
        Set<MockManagedQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 4);
        root.setHardConcurrencyLimit(3);

        int group1Ran = 0;
        int group2Ran = 0;
        for (int i = 0; i < 1000; i++) {
            group1Ran += completeGroupQueries(group1Queries);
            group2Ran += completeGroupQueries(group2Queries);
            root.processQueuedQueries();
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
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(50);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED_FAIR);

        InternalResourceGroup group1 = root.getOrCreateSubGroup("1", true);
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(50);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        group1.setSchedulingWeight(1);

        InternalResourceGroup group2 = root.getOrCreateSubGroup("2", true);
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(50);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(1);

        InternalResourceGroup group3 = root.getOrCreateSubGroup("3", true);
        group3.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group3.setMaxQueuedQueries(50);
        group3.setHardConcurrencyLimit(2);
        group3.setSoftConcurrencyLimit(2);
        group3.setSchedulingWeight(2);

        Set<MockManagedQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 4);
        Set<MockManagedQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 4);
        Set<MockManagedQueryExecution> group3Queries = fillGroupTo(group3, ImmutableSet.of(), 4);
        root.setHardConcurrencyLimit(4);

        int group1Ran = 0;
        int group2Ran = 0;
        int group3Ran = 0;
        for (int i = 0; i < 1000; i++) {
            group1Ran += completeGroupQueries(group1Queries);
            group2Ran += completeGroupQueries(group2Queries);
            group3Ran += completeGroupQueries(group3Queries);
            root.processQueuedQueries();
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
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(50);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED_FAIR);

        InternalResourceGroup group1 = root.getOrCreateSubGroup("1", true);
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(50);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        group1.setSchedulingWeight(1);

        InternalResourceGroup group2 = root.getOrCreateSubGroup("2", true);
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(50);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(2);

        Set<MockManagedQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 4);
        Set<MockManagedQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 4);
        root.setHardConcurrencyLimit(1);

        int group1Ran = 0;
        for (int i = 0; i < 2000; i++) {
            group1Ran += completeGroupQueries(group1Queries);
            completeGroupQueries(group2Queries);
            root.processQueuedQueries();
            group1Queries = fillGroupTo(group1, group1Queries, 4);
            group2Queries = fillGroupTo(group2, group2Queries, 4);
        }

        assertEquals(group1Ran, 1000);
        assertEquals(group1Ran, 1000);
    }

    @Test
    public void testGetInfo()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(40);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a", true);
        rootA.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(2);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b", true);
        rootB.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(2);
        rootB.setSchedulingWeight(2);
        rootB.setSchedulingPolicy(QUERY_PRIORITY);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x", true);
        rootAX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y", true);
        rootAY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(10);

        InternalResourceGroup rootBX = rootB.getOrCreateSubGroup("x", true);
        rootBX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootBX.setMaxQueuedQueries(10);
        rootBX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootBY = rootB.getOrCreateSubGroup("y", true);
        rootBY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootBY.setMaxQueuedQueries(10);
        rootBY.setHardConcurrencyLimit(10);

        // Queue 40 queries (= maxQueuedQueries (40) + maxRunningQueries (0))
        Set<MockManagedQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 10, false);
        queries.addAll(fillGroupTo(rootAY, ImmutableSet.of(), 10, false));
        queries.addAll(fillGroupTo(rootBX, ImmutableSet.of(), 10, true));
        queries.addAll(fillGroupTo(rootBY, ImmutableSet.of(), 10, true));

        ResourceGroupInfo info = root.getInfo();
        assertEquals(info.getNumRunningQueries(), 0);
        assertEquals(info.getNumQueuedQueries(), 40);

        // root.maxRunningQueries = 4, root.a.maxRunningQueries = 2, root.b.maxRunningQueries = 2. Will have 4 queries running and 36 left queued.
        root.setHardConcurrencyLimit(4);
        root.processQueuedQueries();
        info = root.getInfo();
        assertEquals(info.getNumRunningQueries(), 4);
        assertEquals(info.getNumQueuedQueries(), 36);

        // Complete running queries
        Iterator<MockManagedQueryExecution> iterator = queries.iterator();
        while (iterator.hasNext()) {
            MockManagedQueryExecution query = iterator.next();
            if (query.getState() == RUNNING) {
                query.complete();
                iterator.remove();
            }
        }

        // 4 more queries start running, 32 left queued.
        root.processQueuedQueries();
        info = root.getInfo();
        assertEquals(info.getNumRunningQueries(), 4);
        assertEquals(info.getNumQueuedQueries(), 32);

        // root.maxRunningQueries = 10, root.a.maxRunningQueries = 2, root.b.maxRunningQueries = 2. Still only have 4 running queries and 32 left queued.
        root.setHardConcurrencyLimit(10);
        root.processQueuedQueries();
        info = root.getInfo();
        assertEquals(info.getNumRunningQueries(), 4);
        assertEquals(info.getNumQueuedQueries(), 32);

        // root.maxRunningQueries = 10, root.a.maxRunningQueries = 2, root.b.maxRunningQueries = 10. Will have 10 running queries and 26 left queued.
        rootB.setHardConcurrencyLimit(10);
        root.processQueuedQueries();
        info = root.getInfo();
        assertEquals(info.getNumRunningQueries(), 10);
        assertEquals(info.getNumQueuedQueries(), 26);
    }

    @Test
    public void testGetResourceGroupStateInfo()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, GIGABYTE));
        root.setMaxQueuedQueries(40);
        root.setHardConcurrencyLimit(10);
        root.setSchedulingPolicy(WEIGHTED);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a", true);
        rootA.setSoftMemoryLimit(new DataSize(10, MEGABYTE));
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(0);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b", true);
        rootB.setSoftMemoryLimit(new DataSize(5, MEGABYTE));
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(1);
        rootB.setSchedulingWeight(2);
        rootB.setSchedulingPolicy(QUERY_PRIORITY);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x", true);
        rootAX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y", true);
        rootAY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(10);

        Set<MockManagedQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 5, false);
        queries.addAll(fillGroupTo(rootAY, ImmutableSet.of(), 5, false));
        queries.addAll(fillGroupTo(rootB, ImmutableSet.of(), 10, true));

        ResourceGroupInfo rootInfo = root.getResourceGroupInfo(true, true, false);
        assertEquals(rootInfo.getId(), root.getId());
        assertEquals(rootInfo.getState(), CAN_RUN);
        assertEquals(rootInfo.getSoftMemoryLimit(), root.getSoftMemoryLimit());
        assertEquals(rootInfo.getMemoryUsage(), new DataSize(0, BYTE));
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
    public void testGetStaticResourceGroupInfo()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, GIGABYTE));
        root.setMaxQueuedQueries(100);
        root.setHardConcurrencyLimit(10);
        root.setSchedulingPolicy(WEIGHTED);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a", true);
        rootA.setSoftMemoryLimit(new DataSize(10, MEGABYTE));
        rootA.setMaxQueuedQueries(100);
        rootA.setHardConcurrencyLimit(0);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b", true);
        rootB.setSoftMemoryLimit(new DataSize(5, MEGABYTE));
        rootB.setMaxQueuedQueries(100);
        rootB.setHardConcurrencyLimit(1);
        rootB.setSchedulingWeight(2);
        rootB.setSchedulingPolicy(QUERY_PRIORITY);

        // x is a dynamic resource group
        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x", false);
        rootAX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y", true);
        rootAY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(10);

        for (int i = 0; i < 10; i++) {
            InternalResourceGroup subGroup = rootAX.getOrCreateSubGroup("ax" + i, false);
            subGroup.setSoftMemoryLimit(new DataSize(i, MEGABYTE));
            subGroup.setMaxQueuedQueries(10);
            subGroup.setHardConcurrencyLimit(10);
        }

        for (int i = 0; i < 10; i++) {
            fillGroupTo(rootAX.getOrCreateSubGroup("ax" + i, true), ImmutableSet.of(), 1, false);
        }
        fillGroupTo(rootAY, ImmutableSet.of(), 5, false);
        fillGroupTo(rootB, ImmutableSet.of(), 10, true);

        ResourceGroupInfo rootInfo = root.getResourceGroupInfo(false, false, true);
        assertEquals(rootInfo.getId(), root.getId());
        assertNotNull(rootInfo.getSubGroups());
        assertEquals(rootInfo.getSubGroups().size(), 2);
        Optional<ResourceGroupInfo> rootAInfo = getResourceGroupInfoForId(rootA, rootInfo);
        assertTrue(rootAInfo.isPresent());
        assertNotNull(rootAInfo.get().getSubGroups());
        assertEquals(rootAInfo.get().getSubGroups().size(), 1);
        Optional<ResourceGroupInfo> rootAXInfo = getResourceGroupInfoForId(rootAX, rootAInfo.get());
        // dynamic resource groups should not be returned.
        assertFalse(rootAXInfo.isPresent());
        Optional<ResourceGroupInfo> rootAYInfo = getResourceGroupInfoForId(rootAY, rootAInfo.get());
        assertTrue(rootAYInfo.isPresent());
        assertNotNull(rootAYInfo.get().getSubGroups());
        assertEquals(rootAYInfo.get().getSubGroups().size(), 0);
        Optional<ResourceGroupInfo> rootBInfo = getResourceGroupInfoForId(rootB, rootInfo);
        assertTrue(rootBInfo.isPresent());
        assertNotNull(rootBInfo.get().getSubGroups());
        assertEquals(rootBInfo.get().getSubGroups().size(), 0);
    }

    private Optional<ResourceGroupInfo> getResourceGroupInfoForId(InternalResourceGroup rootA, ResourceGroupInfo rootInfo)
    {
        assertNotNull(rootInfo.getSubGroups());
        for (ResourceGroupInfo subGroup : rootInfo.getSubGroups()) {
            if (subGroup.getId().equals(rootA.getId())) {
                return Optional.of(subGroup);
            }
        }
        return Optional.empty();
    }

    @Test
    public void testGetBlockedQueuedQueries()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false);
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(40);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a", true);
        rootA.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(8);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x", true);
        rootAX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(8);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y", true);
        rootAY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(5);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b", true);
        rootB.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(8);

        InternalResourceGroup rootBX = rootB.getOrCreateSubGroup("x", true);
        rootBX.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootBX.setMaxQueuedQueries(10);
        rootBX.setHardConcurrencyLimit(8);

        InternalResourceGroup rootBY = rootB.getOrCreateSubGroup("y", true);
        rootBY.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        rootBY.setMaxQueuedQueries(10);
        rootBY.setHardConcurrencyLimit(5);

        // Queue 40 queries (= maxQueuedQueries (40) + maxRunningQueries (0))
        Set<MockManagedQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 10, false);
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
        root.processQueuedQueries();
        assertEquals(root.getWaitingQueuedQueries(), 0);
        assertEquals(rootA.getWaitingQueuedQueries(), 5);
        assertEquals(rootAX.getWaitingQueuedQueries(), 6);
        assertEquals(rootAY.getWaitingQueuedQueries(), 6);
        assertEquals(rootB.getWaitingQueuedQueries(), 5);
        assertEquals(rootBX.getWaitingQueuedQueries(), 6);
        assertEquals(rootBY.getWaitingQueuedQueries(), 6);
    }

    private static int completeGroupQueries(Set<MockManagedQueryExecution> groupQueries)
    {
        int groupRan = 0;
        for (Iterator<MockManagedQueryExecution> iterator = groupQueries.iterator(); iterator.hasNext(); ) {
            MockManagedQueryExecution query = iterator.next();
            if (query.getState() == RUNNING) {
                query.complete();
                iterator.remove();
                groupRan++;
            }
        }
        return groupRan;
    }

    private static Set<MockManagedQueryExecution> fillGroupTo(InternalResourceGroup group, Set<MockManagedQueryExecution> existingQueries, int count)
    {
        return fillGroupTo(group, existingQueries, count, false);
    }

    private static Set<MockManagedQueryExecution> fillGroupTo(InternalResourceGroup group, Set<MockManagedQueryExecution> existingQueries, int count, boolean queryPriority)
    {
        int existingCount = existingQueries.size();
        Set<MockManagedQueryExecution> queries = new HashSet<>(existingQueries);
        for (int i = 0; i < count - existingCount; i++) {
            MockManagedQueryExecution query = new MockManagedQueryExecution(0, group.getId().toString().replace(".", "") + Integer.toString(i), queryPriority ? i + 1 : 1);
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
                Objects.equals(actual.getMemoryUsage(), expected.getMemoryUsage()));
    }
}
