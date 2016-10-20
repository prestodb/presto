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

import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyNot;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestMergeWindows
{
    private final LocalQueryRunner queryRunner;
    private final WindowFrame commonFrame;
    private final Window windowA;
    private final Window windowB;

    public TestMergeWindows()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());

        commonFrame = new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING),
                Optional.of(new FrameBound(FrameBound.Type.CURRENT_ROW)));

        windowA = new Window(
                ImmutableList.of(new SymbolReference("suppkey")),
                ImmutableList.of(new SortItem(new SymbolReference("orderkey"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.of(commonFrame));

        windowB = new Window(
                ImmutableList.of(new SymbolReference("orderkey")),
                ImmutableList.of(new SortItem(new SymbolReference("shipdate"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.of(commonFrame));
    }

    /**
     * There are two types of tests in here, and they answer two different
     * questions about MergeWindows (MW):
     *
     * 1) Is MW working as it's supposed to be? The tests running the minimal
     * set of optimizers can tell us this.
     * 2) Has some other optimizer changed the plan in such a way that MW no
     * longer merges windows with identical specifications because the plan
     * that MW sees cannot be optimized by MW? The test running the full set
     * of optimizers answers this, though it isn't actually meaningful unless
     * we know the answer to question 1 is "yes".
     *
     * The tests that use only the minimal set of optimizers are closer to true
     * "unit" tests in that they verify the behavior of MW with as few
     * external dependencies as possible. Those dependencies to include the
     * parser and analyzer, so the phrase "unit" tests should be taken with a
     * grain of salt. Using the parser and anayzler instead of creating plan
     * nodes by hand does have a couple of advantages over a true unit test:
     * 1) The tests are more self-maintaining.
     * 2) They're a lot easier to read.
     * 3) It's a lot less typing.
     *
     * The test that runs with all of the optimzers acts as an integration test
     * and ensures that MW is effective when run with the complete set of
     * optimizers.
     */
    @Test
    public void testMergeableWindowsAllOptimizers()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "SUM(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        PlanMatchPattern pattern =
                anyTree(
                        window(ImmutableList.of(
                                functionCall("sum", windowB, false, "quantity")),
                                anyTree(
                                        window(ImmutableList.of(
                                                functionCall("sum", windowA, false, "quantity"),
                                                functionCall("sum", windowA, false, "discount")),
                                                anyNot(WindowNode.class,
                                                        anyTree())))));

        Plan actualPlan = queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql));
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    @Test
    public void testIdenticalWindowSpecificationsABA()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "SUM(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(ImmutableList.of(
                                functionCall("sum", windowB, false, "quantity")),
                                window(ImmutableList.of(
                                        functionCall("sum", windowA, false, "quantity"),
                                        functionCall("sum", windowA, false, "discount")),
                                        anyNot(WindowNode.class)))));
    }

    @Test
    public void testIdenticalWindowSpecificationsABcpA()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "LAG(quantity, 1, 0.0) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(ImmutableList.of(functionCall("sum", windowA, false, "discount")),
                                window(ImmutableList.of(functionCall("lag", windowB, false, "quantity", "*", "*")),
                                        project(
                                                window(ImmutableList.of(
                                                        functionCall("sum", windowA, false, "quantity")),
                                                        any()))))));
    }

    @Test
    public void testIdenticalWindowSpecificationsAAcpA()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "LAG(quantity, 1, 0.0) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(ImmutableList.of(
                                functionCall("sum", windowA, false, "discount"),
                                functionCall("lag", windowA, false, "quantity", "*", "*")),
                                project(
                                        window(ImmutableList.of(
                                                functionCall("sum", windowA, false, "quantity")),
                                                any())))));
    }

    @Test
    public void testIdenticalWindowSpecificationsDefaultFrame()
    {
        Window windowC = new Window(
                ImmutableList.of(new SymbolReference("suppkey")),
                ImmutableList.of(new SortItem(new SymbolReference("orderkey"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.empty());

        Window windowD = new Window(
                ImmutableList.of(new SymbolReference("orderkey")),
                ImmutableList.of(new SortItem(new SymbolReference("shipdate"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.empty());

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION By suppkey ORDER BY orderkey), " +
                "SUM(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate), " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey) " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(ImmutableList.of(
                                functionCall("sum", windowD, false, "quantity")),
                                window(ImmutableList.of(
                                        functionCall("sum", windowC, false, "quantity"),
                                        functionCall("sum", windowC, false, "discount")),
                                        anyNot(WindowNode.class)))));
    }

    @Test
    public void testMergeDifferentFrames()
    {
        WindowFrame frameC = new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING),
                Optional.of(new FrameBound(FrameBound.Type.CURRENT_ROW)));

        Window windowC = new Window(
                ImmutableList.of(new SymbolReference("suppkey")),
                ImmutableList.of(new SortItem(new SymbolReference("orderkey"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.of(frameC));

        WindowFrame frameD = new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.CURRENT_ROW),
                Optional.of(new FrameBound(FrameBound.Type.UNBOUNDED_FOLLOWING)));

        Window windowD = new Window(
                ImmutableList.of(new SymbolReference("suppkey")),
                ImmutableList.of(new SortItem(new SymbolReference("orderkey"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.of(frameD));

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "AVG(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) avg_quantity_D, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_C " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(ImmutableList.of(
                                functionCall("avg", windowD, false, "quantity"),
                                functionCall("sum", windowC, false, "discount"),
                                functionCall("sum", windowC, false, "quantity")),
                                any())));
    }

    @Test
    public void testMergeDifferentFramesWithDefault()
    {
        Window windowC = new Window(
                ImmutableList.of(new SymbolReference("suppkey")),
                ImmutableList.of(new SortItem(new SymbolReference("orderkey"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.empty());

        WindowFrame frameD = new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.CURRENT_ROW),
                Optional.of(new FrameBound(FrameBound.Type.UNBOUNDED_FOLLOWING)));

        Window windowD = new Window(
                ImmutableList.of(new SymbolReference("suppkey")),
                ImmutableList.of(new SortItem(new SymbolReference("orderkey"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.of(frameD));

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey) sum_quantity_C, " +
                "AVG(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) avg_quantity_D, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey) sum_discount_C " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(ImmutableList.of(
                                functionCall("avg", windowD, false, "quantity"),
                                functionCall("sum", windowC, false, "discount"),
                                functionCall("sum", windowC, false, "quantity")),
                                any())));
    }

    @Test
    public void testNotMergeAcrossJoinBranches()
    {
        @Language("SQL") String sql = "WITH foo AS (" +
                "SELECT " +
                "suppkey, orderkey, partkey, " +
                "SUM(discount) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a " +
                "FROM lineitem WHERE (partkey = 272 OR partkey = 273) AND suppkey > 50 " +
                "), " +
                "bar AS ( " +
                "SELECT " +
                "suppkey, orderkey, partkey, " +
                "AVG(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) b " +
                "FROM lineitem WHERE (partkey = 272 OR partkey = 273) AND suppkey > 50 " +
                ")" +
                "SELECT * FROM foo, bar WHERE foo.a = bar.b";

        assertUnitPlan(sql,
                anyTree(
                        join(JoinNode.Type.INNER, ImmutableList.of(),
                                any(
                                        window(ImmutableList.of(functionCall("sum", "*")),
                                                anyTree())),
                                any(
                                        window(ImmutableList.of(functionCall("avg", "*")),
                                                anyTree())))));
    }

    @Test
    public void testNotMergeDifferentPartition()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) over (PARTITION BY quantity ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C " +
                "FROM lineitem";

        Window windowC = new Window(
                ImmutableList.of(new SymbolReference("quantity")),
                ImmutableList.of(new SortItem(new SymbolReference("orderkey"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.of(commonFrame));

        assertUnitPlan(sql,
                anyTree(
                        window(ImmutableList.of(
                                functionCall("sum", windowC, false, "quantity")),
                                window(ImmutableList.of(
                                        functionCall("sum", windowA, false, "extendedprice")),
                                        anyNot(WindowNode.class)))));
    }

    @Test
    public void testNotMergeDifferentOrderBy()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY quantity ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C " +
                "FROM lineitem";

        Window windowC = new Window(
                ImmutableList.of(new SymbolReference("suppkey")),
                ImmutableList.of(new SortItem(new SymbolReference("quantity"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.of(commonFrame));

        assertUnitPlan(sql,
                anyTree(
                        window(ImmutableList.of(
                                functionCall("sum", windowC, false, "quantity")),
                                window(ImmutableList.of(
                                        functionCall("sum", windowA, false, "extendedprice")),
                                        anyNot(WindowNode.class)))));
    }

    @Test
    public void testNotMergeDifferentOrdering()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) over (PARTITION BY suppkey ORDER BY orderkey DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "SUM(discount) over (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        Window windowC = new Window(
                ImmutableList.of(new SymbolReference("suppkey")),
                ImmutableList.of(new SortItem(new SymbolReference("orderkey"), SortItem.Ordering.DESCENDING, SortItem.NullOrdering.UNDEFINED)),
                Optional.of(commonFrame));

        assertUnitPlan(sql,
                anyTree(
                        window(ImmutableList.of(
                                functionCall("sum", windowC, false, "quantity")),
                                window(ImmutableList.of(
                                        functionCall("sum", windowA, false, "extendedprice"),
                                        functionCall("sum", windowA, false, "discount")),
                                        anyNot(WindowNode.class)))));
    }

    @Test
    public void testNotMergeDifferentNullOrdering()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        Window windowC = new Window(
                ImmutableList.of(new SymbolReference("suppkey")),
                ImmutableList.of(new SortItem(new SymbolReference("orderkey"), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.FIRST)),
                Optional.of(commonFrame));

        assertUnitPlan(sql,
                anyTree(
                        window(ImmutableList.of(
                                functionCall("sum", windowC, false, "quantity")),
                                window(ImmutableList.of(
                                        functionCall("sum", windowA, false, "extendedprice"),
                                        functionCall("sum", windowA, false, "discount")),
                                        anyNot(WindowNode.class)))));
    }

    private void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        Plan actualPlan = unitPlan(sql);
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private Plan unitPlan(@Language("SQL") String sql)
    {
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setDistributedIndexJoinsEnabled(false)
                .setOptimizeHashGeneration(true);
        List<PlanOptimizer> optimizers = ImmutableList.of(
                        new UnaliasSymbolReferences(),
                        new PruneIdentityProjections(),
                        new MergeWindows(),
                        new PruneUnreferencedOutputs());
        return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql, featuresConfig, optimizers));
    }
}
