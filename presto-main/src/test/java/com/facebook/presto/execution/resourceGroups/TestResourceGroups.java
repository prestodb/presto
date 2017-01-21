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
import com.facebook.presto.execution.resourceGroups.InternalResourceGroup.RootInternalResourceGroup;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.QUERY_PRIORITY;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Collections.reverse;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestResourceGroups
{
    @Test(timeOut = 10_000)
    public void testQueueFull()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> { }, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(1);
        root.setMaxRunningQueries(1);
        MockQueryExecution query1 = new MockQueryExecution(0);
        root.run(query1);
        assertEquals(query1.getState(), RUNNING);
        MockQueryExecution query2 = new MockQueryExecution(0);
        root.run(query2);
        assertEquals(query2.getState(), QUEUED);
        MockQueryExecution query3 = new MockQueryExecution(0);
        root.run(query3);
        assertEquals(query3.getState(), FAILED);
        assertEquals(query3.getFailureCause().getMessage(), "Too many queued queries for \"root\"");
    }

    @Test(timeOut = 10_000)
    public void testFairEligibility()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> { }, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        root.setMaxRunningQueries(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(4);
        group1.setMaxRunningQueries(1);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(4);
        group2.setMaxRunningQueries(1);
        InternalResourceGroup group3 = root.getOrCreateSubGroup("3");
        group3.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group3.setMaxQueuedQueries(4);
        group3.setMaxRunningQueries(1);
        MockQueryExecution query1a = new MockQueryExecution(0);
        group1.run(query1a);
        assertEquals(query1a.getState(), RUNNING);
        MockQueryExecution query1b = new MockQueryExecution(0);
        group1.run(query1b);
        assertEquals(query1b.getState(), QUEUED);
        MockQueryExecution query2a = new MockQueryExecution(0);
        group2.run(query2a);
        assertEquals(query2a.getState(), QUEUED);
        MockQueryExecution query2b = new MockQueryExecution(0);
        group2.run(query2b);
        assertEquals(query2b.getState(), QUEUED);
        MockQueryExecution query3a = new MockQueryExecution(0);
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

    @Test(timeOut = 10_000)
    public void testFairQueuing()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> { }, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        root.setMaxRunningQueries(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(4);
        group1.setMaxRunningQueries(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(4);
        group2.setMaxRunningQueries(2);
        MockQueryExecution query1a = new MockQueryExecution(0);
        group1.run(query1a);
        assertEquals(query1a.getState(), RUNNING);
        MockQueryExecution query1b = new MockQueryExecution(0);
        group1.run(query1b);
        assertEquals(query1b.getState(), QUEUED);
        MockQueryExecution query1c = new MockQueryExecution(0);
        group1.run(query1c);
        assertEquals(query1c.getState(), QUEUED);
        MockQueryExecution query2a = new MockQueryExecution(0);
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
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> { }, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, BYTE));
        root.setMaxQueuedQueries(4);
        root.setMaxRunningQueries(3);
        MockQueryExecution query1 = new MockQueryExecution(1);
        root.run(query1);
        // Process the group to refresh stats
        root.processQueuedQueries();
        assertEquals(query1.getState(), RUNNING);
        MockQueryExecution query2 = new MockQueryExecution(0);
        root.run(query2);
        assertEquals(query2.getState(), QUEUED);
        MockQueryExecution query3 = new MockQueryExecution(0);
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
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> { }, directExecutor());
        root.setSoftMemoryLimit(new DataSize(10, BYTE));
        root.setMaxQueuedQueries(4);
        root.setMaxRunningQueries(3);
        InternalResourceGroup subgroup = root.getOrCreateSubGroup("subgroup");
        subgroup.setSoftMemoryLimit(new DataSize(1, BYTE));
        subgroup.setMaxQueuedQueries(4);
        subgroup.setMaxRunningQueries(3);

        MockQueryExecution query1 = new MockQueryExecution(1);
        subgroup.run(query1);
        // Process the group to refresh stats
        root.processQueuedQueries();
        assertEquals(query1.getState(), RUNNING);
        MockQueryExecution query2 = new MockQueryExecution(0);
        subgroup.run(query2);
        assertEquals(query2.getState(), QUEUED);
        MockQueryExecution query3 = new MockQueryExecution(0);
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
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> { }, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, BYTE));
        root.setSoftCpuLimit(new Duration(1, SECONDS));
        root.setHardCpuLimit(new Duration(2, SECONDS));
        root.setCpuQuotaGenerationMillisPerSecond(2000);
        root.setMaxQueuedQueries(1);
        root.setMaxRunningQueries(2);

        MockQueryExecution query1 = new MockQueryExecution(1, new Duration(1, SECONDS), 1);
        root.run(query1);
        assertEquals(query1.getState(), RUNNING);

        MockQueryExecution query2 = new MockQueryExecution(0);
        root.run(query2);
        assertEquals(query2.getState(), RUNNING);

        MockQueryExecution query3 = new MockQueryExecution(0);
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
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> { }, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, BYTE));
        root.setHardCpuLimit(new Duration(1, SECONDS));
        root.setCpuQuotaGenerationMillisPerSecond(2000);
        root.setMaxQueuedQueries(1);
        root.setMaxRunningQueries(1);
        MockQueryExecution query1 = new MockQueryExecution(1, new Duration(2, SECONDS), 1);
        root.run(query1);
        assertEquals(query1.getState(), RUNNING);
        MockQueryExecution query2 = new MockQueryExecution(0);
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
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> { }, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(100);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setMaxRunningQueries(0);
        root.setSchedulingPolicy(QUERY_PRIORITY);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(100);
        group1.setMaxRunningQueries(1);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(100);
        group2.setMaxRunningQueries(1);

        SortedMap<Integer, MockQueryExecution> queries = new TreeMap<>();

        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int priority;
            do {
                priority = random.nextInt(1_000_000) + 1;
            } while (queries.containsKey(priority));

            MockQueryExecution query = new MockQueryExecution(0, priority);
            if (random.nextBoolean()) {
                group1.run(query);
            }
            else {
                group2.run(query);
            }
            queries.put(priority, query);
        }

        root.setMaxRunningQueries(1);

        List<MockQueryExecution> orderedQueries = new ArrayList<>(queries.values());
        reverse(orderedQueries);

        for (MockQueryExecution query : orderedQueries) {
            root.processQueuedQueries();
            assertEquals(query.getState(), RUNNING);
            query.complete();
        }
    }

    @Test(timeOut = 10_000)
    public void testWeightedScheduling()
    {
        RootInternalResourceGroup root = new RootInternalResourceGroup("root", (group, export) -> { }, directExecutor());
        root.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        root.setMaxQueuedQueries(4);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setMaxRunningQueries(0);
        root.setSchedulingPolicy(WEIGHTED);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(2);
        group1.setMaxRunningQueries(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(2);
        group2.setMaxRunningQueries(2);
        group2.setSchedulingWeight(2);

        Set<MockQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 2);
        Set<MockQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 2);
        root.setMaxRunningQueries(1);

        int group2Ran = 0;
        for (int i = 0; i < 1000; i++) {
            for (Iterator<MockQueryExecution> iterator = group1Queries.iterator(); iterator.hasNext(); ) {
                MockQueryExecution query = iterator.next();
                if (query.getState() == RUNNING) {
                    query.complete();
                    iterator.remove();
                }
            }
            for (Iterator<MockQueryExecution> iterator = group2Queries.iterator(); iterator.hasNext(); ) {
                MockQueryExecution query = iterator.next();
                if (query.getState() == RUNNING) {
                    query.complete();
                    iterator.remove();
                    group2Ran++;
                }
            }
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

    private static Set<MockQueryExecution> fillGroupTo(InternalResourceGroup group, Set<MockQueryExecution> existingQueries, int count)
    {
        Set<MockQueryExecution> queries = new HashSet<>(existingQueries);
        while (queries.size() < count) {
            MockQueryExecution query = new MockQueryExecution(0);
            queries.add(query);
            group.run(query);
        }
        return queries;
    }
}
