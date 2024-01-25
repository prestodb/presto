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
package com.facebook.presto.nativeworker;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeQueryRunnerParameters;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestNativeSystemQueries
        extends AbstractTestQueryFramework
{
    @Test
    public void testNodes()
    {
        int workerCount = getNativeQueryRunnerParameters().workerCount.orElse(4);
        assertQueryResultCount("select * from system.runtime.nodes where coordinator = false", workerCount);
    }

    @Test
    public void testTasks()
    {
        assertQueryFails("select * from system.runtime.tasks",
                ".*system not registered.*");
    }

    @Test
    public void testQueries()
    {
        assertQueryGteResultCount("select * from system.runtime.queries", 1);
    }

    @Test
    public void testTransactions()
    {
        assertQueryGteResultCount("select * from system.runtime.transactions", 1);
    }

    private void assertQueryResultCount(String sql, int expectedResultCount)
    {
        assertEquals(getQueryRunner().execute(sql).getRowCount(), expectedResultCount);
    }

    private void assertQueryGteResultCount(String sql, int gteResultCount)
    {
        assertGreaterThanOrEqual(getQueryRunner().execute(sql).getRowCount(), gteResultCount);
    }
}
