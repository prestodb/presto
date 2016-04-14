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
import com.facebook.presto.execution.resourceGroups.ResourceGroup.RootResourceGroup;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestResourceGroups
{
    @Test(timeOut = 10_000)
    public void testFairEligibility()
    {
        RootResourceGroup root = new RootResourceGroup("root", 1, 4, new DataSize(1, MEGABYTE), MoreExecutors.directExecutor());
        ResourceGroup group1 = root.getOrCreateSubGroup("1", 1, 4, new DataSize(1, MEGABYTE));
        ResourceGroup group2 = root.getOrCreateSubGroup("2", 1, 4, new DataSize(1, MEGABYTE));
        ResourceGroup group3 = root.getOrCreateSubGroup("3", 1, 4, new DataSize(1, MEGABYTE));
        MockQueryExecution query1a = new MockQueryExecution(0);
        group1.add(query1a);
        assertEquals(query1a.getState(), RUNNING);
        MockQueryExecution query1b = new MockQueryExecution(0);
        group1.add(query1b);
        assertEquals(query1b.getState(), QUEUED);
        MockQueryExecution query2a = new MockQueryExecution(0);
        group2.add(query2a);
        assertEquals(query2a.getState(), QUEUED);
        MockQueryExecution query2b = new MockQueryExecution(0);
        group2.add(query2b);
        assertEquals(query2b.getState(), QUEUED);
        MockQueryExecution query3a = new MockQueryExecution(0);
        group3.add(query3a);
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
        RootResourceGroup root = new RootResourceGroup("root", 1, 4, new DataSize(1, MEGABYTE), MoreExecutors.directExecutor());
        ResourceGroup group1 = root.getOrCreateSubGroup("1", 2, 4, new DataSize(1, MEGABYTE));
        ResourceGroup group2 = root.getOrCreateSubGroup("2", 2, 4, new DataSize(1, MEGABYTE));
        MockQueryExecution query1a = new MockQueryExecution(0);
        group1.add(query1a);
        assertEquals(query1a.getState(), RUNNING);
        MockQueryExecution query1b = new MockQueryExecution(0);
        group1.add(query1b);
        assertEquals(query1b.getState(), QUEUED);
        MockQueryExecution query1c = new MockQueryExecution(0);
        group1.add(query1c);
        assertEquals(query1c.getState(), QUEUED);
        MockQueryExecution query2a = new MockQueryExecution(0);
        group2.add(query2a);
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
        RootResourceGroup root = new RootResourceGroup("root", 3, 4, new DataSize(1, BYTE), MoreExecutors.directExecutor());
        MockQueryExecution query1 = new MockQueryExecution(1);
        root.add(query1);
        // Process the group to refresh stats
        root.processQueuedQueries();
        assertEquals(query1.getState(), RUNNING);
        MockQueryExecution query2 = new MockQueryExecution(0);
        root.add(query2);
        assertEquals(query2.getState(), QUEUED);
        MockQueryExecution query3 = new MockQueryExecution(0);
        root.add(query3);
        assertEquals(query3.getState(), QUEUED);

        query1.complete();
        root.processQueuedQueries();
        assertEquals(query2.getState(), RUNNING);
        assertEquals(query3.getState(), RUNNING);
    }
}
