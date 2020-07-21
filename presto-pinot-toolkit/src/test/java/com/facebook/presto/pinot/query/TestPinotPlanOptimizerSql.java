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
package com.facebook.presto.pinot.query;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class TestPinotPlanOptimizerSql
        extends TestPinotPlanOptimizer
{
    @Override
    public boolean useSqlSyntax()
    {
        return true;
    }

    @Test
    public void assertUsingSqlSyntax()
    {
        Assert.assertEquals(defaultSessionHolder.getConnectorSession().getProperty("use_pinot_sql_for_broker_queries", Boolean.class).booleanValue(), true);
    }

    @Test
    public void testDistinctLimitPushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        PlanNode originalPlan = distinctLimit(
                planBuilder,
                ImmutableList.of(new VariableReferenceExpression("regionid", BIGINT)),
                50L,
                tableScan(planBuilder, pinotTable, regionId));
        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan);
        assertPlanMatch(
                optimized,
                PinotTableScanMatcher.match(
                        pinotTable,
                        Optional.of("SELECT regionId FROM hybrid GROUP BY regionId LIMIT 50"),
                        Optional.of(false),
                        originalPlan.getOutputVariables(),
                        useSqlSyntax()),
                typeProvider);

        planBuilder = createPlanBuilder(defaultSessionHolder);
        originalPlan = distinctLimit(
                planBuilder,
                ImmutableList.of(
                        new VariableReferenceExpression("regionid", BIGINT),
                        new VariableReferenceExpression("city", VARCHAR)),
                50L,
                tableScan(planBuilder, pinotTable, regionId, city));
        optimized = getOptimizedPlan(planBuilder, originalPlan);
        assertPlanMatch(
                optimized,
                PinotTableScanMatcher.match(
                        pinotTable,
                        Optional.of("SELECT regionId, city FROM hybrid GROUP BY regionId, city LIMIT 50"),
                        Optional.of(false),
                        originalPlan.getOutputVariables(),
                        useSqlSyntax()),
                typeProvider);
    }
}
