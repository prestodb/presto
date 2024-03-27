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

import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.facebook.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeQueryRunnerParameters;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestNativeSystemQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected FeaturesConfig createFeaturesConfig()
    {
        return new FeaturesConfig().setNativeExecutionEnabled(true);
    }

    @Test
    public void testNodes()
    {
        int workerCount = getNativeQueryRunnerParameters().workerCount.orElse(4);
        assertQueryResultCount("select * from system.runtime.nodes where coordinator = false", workerCount);
    }

    @Test
    public void testTasks()
    {
        // This query has a single task on the co-ordinator, and at least one on a Native worker.
        // So limit 2 should always return correctly.
        assertQueryResultCount("select * from system.runtime.tasks limit 2", 2);

        // This query performs an aggregation on tasks table.
        // There are 2 special planning rules for system tables :
        // i) Partial aggregations are disabled for system tables
        // (as aggregations are not consistent across co-ordinator and native workers)
        // ii) A remote gather exchange is added after the system table TableScanNode so that the partitioning
        // is made consistent with the rest of the tables.
        String aggregation = "select sum(output_bytes) from system.runtime.tasks";
        assertQuerySucceeds(aggregation);
        assertPlan(
                aggregation,
                anyTree(
                        aggregation(
                                Collections.emptyMap(),
                                SINGLE,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, GATHER,
                                                tableScan("tasks"))))));
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
