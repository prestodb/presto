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
package io.prestosql.cost;

import com.google.common.collect.ImmutableMap;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.assertions.PlanAssert;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestStatsCalculator
{
    private final LocalQueryRunner queryRunner;

    public TestStatsCalculator()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .build());

        queryRunner.createCatalog(
                queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    @Test
    public void testStatsCalculatorUsesLayout()
    {
        assertPlan("SELECT orderstatus FROM orders WHERE orderstatus = 'P'",
                anyTree(node(TableScanNode.class).withOutputRowCount(363.0)));

        assertPlan("SELECT orderstatus FROM orders WHERE orderkey = 42",
                anyTree(node(TableScanNode.class).withOutputRowCount(15000.0)));
    }

    private void assertPlan(String sql, PlanMatchPattern pattern)
    {
        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, pattern);
    }

    private void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, stage, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }
}
