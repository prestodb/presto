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
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestResourceGroupQueuedOn
{
    @Test(timeOut = 10_000)
    public void testQueuedOnLeaf()
    {
        /**
         * Leaf has hard concurrency limit of 1. If a query is already running on the leaf, the next query should be queued.
         */
        InternalResourceGroup.RootInternalResourceGroup parent = new InternalResourceGroup.RootInternalResourceGroup(
                "parent",
                (group, export) -> {}, directExecutor(),
                ignored -> Optional.empty(),
                rg -> false);
        parent.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        parent.setMaxQueuedQueries(2);
        parent.setHardConcurrencyLimit(1);

        InternalResourceGroup subgroup1 = parent.getOrCreateSubGroup("subgroup1", true);
        subgroup1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        subgroup1.setMaxQueuedQueries(1);
        subgroup1.setHardConcurrencyLimit(1);

        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        query1.startWaitingForPrerequisites();
        subgroup1.run(query1);

        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        subgroup1.run(query2);

        assertEquals(query2.getResourceGroupQueuedOn(), Optional.of(subgroup1.getId()));
    }

    @Test(timeOut = 10_000)
    public void testQueuedOnParent()
    {
        /**
         * Parent has hard concurrency limit of 1. If a query is running on subgroup1, a query submitted to subgroup2 should be queued on the parent's hard concurrency limit.
         */
        InternalResourceGroup.RootInternalResourceGroup parent = new InternalResourceGroup.RootInternalResourceGroup(
                "parent",
                (group, export) -> {}, directExecutor(),
                ignored -> Optional.empty(),
                rg -> false);
        parent.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        parent.setMaxQueuedQueries(2);
        parent.setHardConcurrencyLimit(1);

        InternalResourceGroup subgroup1 = parent.getOrCreateSubGroup("subgroup1", true);
        subgroup1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        subgroup1.setMaxQueuedQueries(1);
        subgroup1.setHardConcurrencyLimit(2);

        InternalResourceGroup subgroup2 = parent.getOrCreateSubGroup("subgroup2", true);
        subgroup2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        subgroup2.setMaxQueuedQueries(1);
        subgroup2.setHardConcurrencyLimit(2);

        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        query1.startWaitingForPrerequisites();
        subgroup1.run(query1);

        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        subgroup2.run(query2);

        assertEquals(query2.getResourceGroupQueuedOn(), Optional.of(parent.getId()));
    }

    @Test(timeOut = 10_000)
    public void testQueuedOnGrandparent()
    {
        /**
         * Grandparent has hard concurrency limit of 1. If parent has hard concurrency limit of 2 and a query is running on subgroup1, a query submitted to subgroup2 should be
         * queued on the grandparent's hard concurrency limit.
         */
        InternalResourceGroup.RootInternalResourceGroup grandparent = new InternalResourceGroup.RootInternalResourceGroup(
                "grandparent",
                (group, export) -> {}, directExecutor(),
                ignored -> Optional.empty(),
                rg -> false);
        grandparent.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        grandparent.setMaxQueuedQueries(2);
        grandparent.setHardConcurrencyLimit(1);

        InternalResourceGroup parent = grandparent.getOrCreateSubGroup("parent", true);
        parent.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        parent.setMaxQueuedQueries(2);
        parent.setHardConcurrencyLimit(2);

        InternalResourceGroup subgroup1 = parent.getOrCreateSubGroup("subgroup1", true);
        subgroup1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        subgroup1.setMaxQueuedQueries(2);
        subgroup1.setHardConcurrencyLimit(2);

        InternalResourceGroup subgroup2 = parent.getOrCreateSubGroup("subgroup2", true);
        subgroup2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        subgroup2.setMaxQueuedQueries(2);
        subgroup2.setHardConcurrencyLimit(2);

        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        query1.startWaitingForPrerequisites();
        subgroup1.run(query1);

        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        subgroup2.run(query2);

        assertEquals(query2.getResourceGroupQueuedOn(), Optional.of(grandparent.getId()));
    }

    @Test(timeOut = 10_000)
    public void testCanRun()
    {
        /**
         * Query can run, and is not queued on any ancestors. resourceGroupQueuedOn should be empty.
         */
        InternalResourceGroup.RootInternalResourceGroup parent = new InternalResourceGroup.RootInternalResourceGroup(
                "parent",
                (group, export) ->
                {},
                directExecutor(),
                ignored -> Optional.empty(),
                rg -> false);
        parent.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        parent.setMaxQueuedQueries(2);
        parent.setHardConcurrencyLimit(2);
        InternalResourceGroup group1 = parent.getOrCreateSubGroup("child1", true);
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(1);
        group1.setHardConcurrencyLimit(2);
        InternalResourceGroup group2 = parent.getOrCreateSubGroup("child2", true);
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(1);
        group2.setHardConcurrencyLimit(2);

        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        query1.startWaitingForPrerequisites();
        group1.run(query1);

        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        group2.run(query2);

        assertEquals(query2.getResourceGroupQueuedOn(), Optional.empty());
    }

    @Test(timeOut = 10_000)
    public void testFailedOnLeaf()
    {
        /**
         * Leaf has hard concurrency limit of 1 and max queued queries limit of 0. If a query is running on subgroup1, the next query submitted to subgroup1 should be fail.
         * resourceGroupQueuedOn should be empty.
         */
        InternalResourceGroup.RootInternalResourceGroup parent = new InternalResourceGroup.RootInternalResourceGroup(
                "parent",
                (group, export) -> {}, directExecutor(),
                ignored -> Optional.empty(),
                rg -> false);
        parent.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        parent.setMaxQueuedQueries(2);
        parent.setHardConcurrencyLimit(1);

        InternalResourceGroup subgroup1 = parent.getOrCreateSubGroup("subgroup1", true);
        subgroup1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        subgroup1.setMaxQueuedQueries(0);
        subgroup1.setHardConcurrencyLimit(1);

        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        query1.startWaitingForPrerequisites();
        subgroup1.run(query1);

        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        subgroup1.run(query2);

        assertEquals(query2.getResourceGroupQueuedOn(), Optional.empty());
    }

    @Test(timeOut = 10_000)
    public void testFailedOnParent()
    {
        /**
         * Parent has hard concurrency limit of 1 and max queued queries limit of 0. If a query is running on subgroup1, a query submitted to subgroup2 should be fail.
         * resourceGroupQueuedOn should be empty.
         */
        InternalResourceGroup.RootInternalResourceGroup parent = new InternalResourceGroup.RootInternalResourceGroup(
                "parent",
                (group, export) ->
                {},
                directExecutor(),
                ignored -> Optional.empty(),
                rg -> false);
        parent.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        parent.setMaxQueuedQueries(0);
        parent.setHardConcurrencyLimit(1);
        InternalResourceGroup group1 = parent.getOrCreateSubGroup("child1", true);
        group1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group1.setMaxQueuedQueries(1);
        group1.setHardConcurrencyLimit(2);
        InternalResourceGroup group2 = parent.getOrCreateSubGroup("child2", true);
        group2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        group2.setMaxQueuedQueries(1);
        group2.setHardConcurrencyLimit(2);

        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        query1.startWaitingForPrerequisites();
        group1.run(query1);

        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        group2.run(query2);

        assertEquals(query2.getResourceGroupQueuedOn(), Optional.empty());
    }

    @Test(timeOut = 10_000)
    public void testFailedOnGrandparent()
    {
        /**
         * Grandparent has hard concurrency limit of 1 and max queued queries limit of 0. If a query is running on subgroup1, a query submitted to subgroup2 should be fail.
         * resourceGroupQueuedOn should be empty.
         */
        InternalResourceGroup.RootInternalResourceGroup grandparent = new InternalResourceGroup.RootInternalResourceGroup(
                "grandparent",
                (group, export) -> {}, directExecutor(),
                ignored -> Optional.empty(),
                rg -> false);
        grandparent.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        grandparent.setMaxQueuedQueries(0);
        grandparent.setHardConcurrencyLimit(1);

        InternalResourceGroup parent = grandparent.getOrCreateSubGroup("parent", true);
        parent.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        parent.setMaxQueuedQueries(2);
        parent.setHardConcurrencyLimit(2);

        InternalResourceGroup subgroup1 = parent.getOrCreateSubGroup("subgroup1", true);
        subgroup1.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        subgroup1.setMaxQueuedQueries(2);
        subgroup1.setHardConcurrencyLimit(2);

        InternalResourceGroup subgroup2 = parent.getOrCreateSubGroup("subgroup2", true);
        subgroup2.setSoftMemoryLimit(new DataSize(1, MEGABYTE));
        subgroup2.setMaxQueuedQueries(2);
        subgroup2.setHardConcurrencyLimit(2);

        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        query1.startWaitingForPrerequisites();
        subgroup1.run(query1);

        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        query2.startWaitingForPrerequisites();
        subgroup2.run(query2);

        assertEquals(query2.getResourceGroupQueuedOn(), Optional.empty());
    }
}
