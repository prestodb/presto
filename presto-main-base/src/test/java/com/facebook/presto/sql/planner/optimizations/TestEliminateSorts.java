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

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestEliminateSorts
        extends BasePlanTest
{
    private static final String QUANTITY_ALIAS = "QUANTITY";
    private static final String TAX_ALIAS = "TAX";

    private static final ExpectedValueProvider<DataOrganizationSpecification> windowSpec = specification(
            ImmutableList.of(),
            ImmutableList.of(QUANTITY_ALIAS),
            ImmutableMap.of(QUANTITY_ALIAS, SortOrder.ASC_NULLS_LAST));

    private static final PlanMatchPattern LINEITEM_TABLESCAN_Q_BASIC = tableScan(
            "lineitem",
            ImmutableMap.of(QUANTITY_ALIAS, "quantity"));

    private static final PlanMatchPattern LINEITEM_TABLESCAN_Q = tableScan(
            "lineitem",
            ImmutableMap.of(QUANTITY_ALIAS, "quantity", TAX_ALIAS, "tax"));

    @Test
    public void testEliminateSortsBasic()
    {
        @Language("SQL") String sql = "SELECT quantity, row_number() OVER (ORDER BY quantity) FROM lineitem ORDER BY quantity";

        PlanMatchPattern pattern =
                output(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(windowSpec)
                                        .addFunction(functionCall("row_number", Optional.empty(), ImmutableList.of())),
                                anyTree(LINEITEM_TABLESCAN_Q_BASIC)));

        assertUnitPlan(sql, pattern);
    }

    /**
     * Cannot eliminate sorts when there is a filter or project above the window as a local exchange
     * will be added to repartition the data before the filter or project node
     */
    @Test
    public void testDoesNotEliminateSortsWithFilter()
    {
        @Language("SQL") String sql = "SELECT * FROM " +
                "(SELECT quantity, count(tax) OVER (ORDER BY quantity) AS c  FROM lineitem) " +
                "WHERE c > 3 ORDER BY quantity";

        PlanMatchPattern pattern =
                output(
                        exchange(
                                LOCAL,
                                GATHER,
                                sort(
                                        filter("C > 3",
                                                exchange(
                                                        LOCAL,
                                                        REPARTITION,
                                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                                        .specification(windowSpec)
                                                                        .addFunction(
                                                                                "C",
                                                                                functionCall("count", Optional.empty(), ImmutableList.of(TAX_ALIAS))),
                                                                anyTree(LINEITEM_TABLESCAN_Q)))))));

        assertUnitPlan(sql, pattern);
    }

    @Test
    public void testNotEliminateSorts()
    {
        @Language("SQL") String sql = "SELECT quantity, row_number() OVER (ORDER BY quantity) FROM lineitem ORDER BY tax";

        PlanMatchPattern pattern =
                anyTree(
                        sort(
                                anyTree(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(windowSpec)
                                                        .addFunction(functionCall("row_number", Optional.empty(), ImmutableList.of())),
                                                anyTree(LINEITEM_TABLESCAN_Q)))));

        assertUnitPlan(sql, pattern);
    }

    public void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(getMetadata().getFunctionAndTypeManager()),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        getMetadata(),
                        new RuleStatsRecorder(),
                        getQueryRunner().getStatsCalculator(),
                        getQueryRunner().getCostCalculator(),
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new AddExchanges(getQueryRunner().getMetadata(), new PartitioningProviderManager(), false),
                new AddLocalExchanges(getMetadata(), false),
                new UnaliasSymbolReferences(getMetadata().getFunctionAndTypeManager()),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        getMetadata(),
                        new RuleStatsRecorder(),
                        getQueryRunner().getStatsCalculator(),
                        getQueryRunner().getCostCalculator(),
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())));
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .build();
        assertPlan(sql, session, OPTIMIZED, pattern, optimizers);
    }
}
