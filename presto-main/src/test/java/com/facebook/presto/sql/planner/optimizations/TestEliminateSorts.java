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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.StatsRecorder;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;

public class TestEliminateSorts
        extends BasePlanTest
{
    private static final String QUANTITY_ALIAS = "QUANTITY";

    private static final ExpectedValueProvider<WindowNode.Specification> windowSpec = specification(
            ImmutableList.of(),
            ImmutableList.of(QUANTITY_ALIAS),
            ImmutableMap.of(QUANTITY_ALIAS, SortOrder.ASC_NULLS_LAST));

    private static final PlanMatchPattern LINEITEM_TABLESCAN_Q = tableScan(
            "lineitem",
            ImmutableMap.of(QUANTITY_ALIAS, "quantity"));

    @Test
    public void testEliminateSorts()
    {
        @Language("SQL") String sql = "SELECT quantity, row_number() OVER (ORDER BY quantity) FROM lineitem ORDER BY quantity";

        PlanMatchPattern pattern =
                output(
                        window(windowSpec,
                                ImmutableList.of(functionCall("row_number", Optional.empty(), ImmutableList.of())),
                                anyTree(LINEITEM_TABLESCAN_Q)));

        assertUnitPlan(sql, pattern);
    }

    @Test
    public void testNotEliminateSorts()
    {
        @Language("SQL") String sql = "SELECT quantity, row_number() OVER (ORDER BY quantity) FROM lineitem ORDER BY tax";

        PlanMatchPattern pattern =
                anyTree(
                        sort(
                                window(windowSpec,
                                        ImmutableList.of(functionCall("row_number", Optional.empty(), ImmutableList.of())),
                                        anyTree(LINEITEM_TABLESCAN_Q))));

        assertUnitPlan(sql, pattern);
    }

    public void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        LocalQueryRunner queryRunner = getQueryRunner();
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new AddExchanges(queryRunner.getMetadata(), new SqlParser()),
                new PruneUnreferencedOutputs(),
                new MergeProjections(),
                new IterativeOptimizer(new StatsRecorder(), ImmutableSet.of(new RemoveRedundantIdentityProjections()))
        );

        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }
}
