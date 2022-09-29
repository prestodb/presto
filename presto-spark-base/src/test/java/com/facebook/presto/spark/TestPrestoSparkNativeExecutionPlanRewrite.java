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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.NativeExecutionNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoSparkNativeExecutionPlanRewrite
        extends AbstractTestQueryFramework
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    private void assertPlanMatch(Session session, PlanNode actual, PlanMatchPattern expected)
    {
        PlanAssert.assertPlan(
                session,
                METADATA,
                (node, sourceStats, lookup, session2, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(actual, TypeProvider.empty(), StatsAndCosts.empty()),
                expected);
    }

    private void assertPlanNotMatch(Session session, PlanNode actual, PlanMatchPattern expected)
    {
        PlanAssert.assertPlanDoesNotMatch(
                session,
                METADATA,
                (node, sourceStats, lookup, session2, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(actual, TypeProvider.empty(), StatsAndCosts.empty()),
                expected);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner();
    }

    @Test
    public void testSingleStagePlanFragment()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .build();

        SubPlan subPlan = subplan(
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment FROM orders_bucketed",
                session);
        PlanFragment fragment = subPlan.getFragment();
        PlanNode root = fragment.getRoot();

        assertEquals(1, fragment.getTableScanSchedulingOrder().size());
        assertTrue(fragment.getTableScanSchedulingOrder().contains(root.getId()));
        assertEquals(1, subPlan.getAllFragments().size());
        assertTrue(root instanceof NativeExecutionNode);
        assertPlanMatch(session, ((NativeExecutionNode) root).getSubPlan(), anyTree(node(TableScanNode.class)));
    }

    @Test
    public void testMultiStagePlanFragmentsWithCoordinatorOnlyFragment()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty("table_writer_merge_operator_enabled", "false")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                .build();

        SubPlan subPlan = subplan("CREATE TABLE test_table_1 as SELECT orderkey, custkey FROM orders ", session);
        List<PlanFragment> fragmentList = subPlan.getAllFragments();

        assertEquals(2, fragmentList.size());

        PlanFragment fragment1 = fragmentList.get(0);
        assertTrue(fragment1.getPartitioning().isCoordinatorOnly());
        assertPlanNotMatch(session, fragment1.getRoot(), anyTree(node(NativeExecutionNode.class)));

        PlanFragment fragment2 = fragmentList.get(1);
        PlanNode root = fragment2.getRoot();
        assertFalse(fragment2.getPartitioning().isCoordinatorOnly());
        assertEquals(1, fragment2.getTableScanSchedulingOrder().size());
        assertTrue(fragment2.getTableScanSchedulingOrder().contains(root.getId()));
        assertTrue(root instanceof NativeExecutionNode);
        assertPlanMatch(session, ((NativeExecutionNode) root).getSubPlan(), anyTree(node(TableScanNode.class)));
    }
}
