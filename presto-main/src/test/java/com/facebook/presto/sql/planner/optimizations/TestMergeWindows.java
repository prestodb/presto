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
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.StatsRecorder;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyNot;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;

public class TestMergeWindows
        extends BasePlanTest
{
    private static final String SUPPKEY_ALIAS = "SUPPKEY";
    private static final String ORDERKEY_ALIAS = "ORDERKEY";
    private static final String SHIPDATE_ALIAS = "SHIPDATE";
    private static final String QUANTITY_ALIAS = "QUANTITY";
    private static final String DISCOUNT_ALIAS = "DISCOUNT";
    private static final String EXTENDEDPRICE_ALIAS = "EXTENDEDPRICE";

    private static final PlanMatchPattern LINEITEM_TABLESCAN_DOQSS = tableScan(
            "lineitem",
            ImmutableMap.of(QUANTITY_ALIAS, "quantity",
                    DISCOUNT_ALIAS, "discount",
                    SUPPKEY_ALIAS, "suppkey",
                    ORDERKEY_ALIAS, "orderkey",
                    SHIPDATE_ALIAS, "shipdate"));

    private static final PlanMatchPattern LINEITEM_TABLESCAN_DOQS = tableScan(
            "lineitem",
            ImmutableMap.of(QUANTITY_ALIAS, "quantity",
                    DISCOUNT_ALIAS, "discount",
                    SUPPKEY_ALIAS, "suppkey",
                    ORDERKEY_ALIAS, "orderkey"));

    private static final PlanMatchPattern LINEITEM_TABLESCAN_DEOQS = tableScan(
            "lineitem",
            ImmutableMap.of(QUANTITY_ALIAS, "quantity",
                    SUPPKEY_ALIAS, "suppkey",
                    ORDERKEY_ALIAS, "orderkey",
                    DISCOUNT_ALIAS, "discount",
                    EXTENDEDPRICE_ALIAS, "extendedprice"));

    private static final Optional<WindowFrame> COMMON_FRAME = Optional.of(new WindowFrame(
        WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING),
                Optional.of(new FrameBound(FrameBound.Type.CURRENT_ROW))));

    private static final Optional<WindowFrame> UNSPECIFIED_FRAME = Optional.empty();

    private final ExpectedValueProvider<WindowNode.Specification> specificationA;
    private final ExpectedValueProvider<WindowNode.Specification> specificationB;

    public TestMergeWindows()
    {
        specificationA = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        specificationB = specification(
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableList.of(SHIPDATE_ALIAS),
                ImmutableMap.of(SHIPDATE_ALIAS, SortOrder.ASC_NULLS_LAST));
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
                        window(specificationA,
                                ImmutableList.of(
                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS)),
                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(DISCOUNT_ALIAS))),
                                anyTree(
                                        window(specificationB,
                                                ImmutableList.of(
                                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS))),
                                                anyNot(WindowNode.class,
                                                        LINEITEM_TABLESCAN_DOQSS)))));  // should be anyTree(LINEITEM_TABLESCAN_DOQSS) but anyTree does not handle zero nodes case correctly

        assertPlan(sql, pattern);
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
                        window(specificationB,
                                ImmutableList.of(
                                functionCall("sum", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS))),
                                window(specificationA,
                                        ImmutableList.of(
                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS)),
                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(DISCOUNT_ALIAS))),
                                        LINEITEM_TABLESCAN_DOQSS))));
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
                        window(specificationA,
                                ImmutableList.of(functionCall("sum", COMMON_FRAME, ImmutableList.of(DISCOUNT_ALIAS))),
                                window(specificationB,
                                        ImmutableList.of(functionCall("lag", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS, "ONE", "ZERO"))),
                                        project(ImmutableMap.of("ONE", expression("CAST(1 AS bigint)"), "ZERO", expression("0.0")),
                                                window(specificationA,
                                                        ImmutableList.of(
                                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS))),
                                                        LINEITEM_TABLESCAN_DOQSS))))));
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
                        window(specificationA,
                                ImmutableList.of(
                                functionCall("sum", COMMON_FRAME, ImmutableList.of(DISCOUNT_ALIAS)),
                                functionCall("lag", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS, "ONE", "ZERO"))),
                                project(ImmutableMap.of("ONE", expression("CAST(1 AS bigint)"), "ZERO", expression("0.0")),
                                        window(specificationA,
                                                ImmutableList.of(
                                                functionCall("sum", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS))),
                                                LINEITEM_TABLESCAN_DOQS)))));
    }

    @Test
    public void testIdenticalWindowSpecificationsDefaultFrame()
    {
        ExpectedValueProvider<WindowNode.Specification> specificationC = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        ExpectedValueProvider<WindowNode.Specification> specificationD = specification(
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableList.of(SHIPDATE_ALIAS),
                ImmutableMap.of(SHIPDATE_ALIAS, SortOrder.ASC_NULLS_LAST));

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION By suppkey ORDER BY orderkey), " +
                "SUM(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate), " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey) " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(specificationD,
                                ImmutableList.of(
                                functionCall("sum", UNSPECIFIED_FRAME, ImmutableList.of(QUANTITY_ALIAS))),
                                window(specificationC,
                                        ImmutableList.of(
                                        functionCall("sum", UNSPECIFIED_FRAME, ImmutableList.of(QUANTITY_ALIAS)),
                                        functionCall("sum", UNSPECIFIED_FRAME, ImmutableList.of(DISCOUNT_ALIAS))),
                                        LINEITEM_TABLESCAN_DOQSS))));
    }

    @Test
    public void testMergeDifferentFrames()
    {
        Optional<WindowFrame> frameC = Optional.of(new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING),
                Optional.of(new FrameBound(FrameBound.Type.CURRENT_ROW))));

        ExpectedValueProvider<WindowNode.Specification> specificationC = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        Optional<WindowFrame> frameD = Optional.of(new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.CURRENT_ROW),
                Optional.of(new FrameBound(FrameBound.Type.UNBOUNDED_FOLLOWING))));

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "AVG(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) avg_quantity_D, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_C " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(specificationC,
                                ImmutableList.of(
                                functionCall("avg", frameD, ImmutableList.of(QUANTITY_ALIAS)),
                                functionCall("sum", frameC, ImmutableList.of(DISCOUNT_ALIAS)),
                                functionCall("sum", frameC, ImmutableList.of(QUANTITY_ALIAS))),
                                LINEITEM_TABLESCAN_DOQS)));
    }

    @Test
    public void testMergeDifferentFramesWithDefault()
    {
        Optional<WindowFrame> frameD = Optional.of(new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.CURRENT_ROW),
                Optional.of(new FrameBound(FrameBound.Type.UNBOUNDED_FOLLOWING))));

        ExpectedValueProvider<WindowNode.Specification> specificationD = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey) sum_quantity_C, " +
                "AVG(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) avg_quantity_D, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey) sum_discount_C " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(specificationD,
                                ImmutableList.of(
                                functionCall("avg", frameD, ImmutableList.of(QUANTITY_ALIAS)),
                                functionCall("sum", UNSPECIFIED_FRAME, ImmutableList.of(DISCOUNT_ALIAS)),
                                functionCall("sum", UNSPECIFIED_FRAME, ImmutableList.of(QUANTITY_ALIAS))),
                                LINEITEM_TABLESCAN_DOQS)));
    }

    @Test
    public void testNotMergeAcrossJoinBranches()
    {
        String rOrderkeyAlias = "R_ORDERKEY";
        String rShipdateAlias = "R_SHIPDATE";
        String rQuantityAlias = "R_QUANTITY";

        @Language("SQL") String sql = "WITH foo AS (" +
                "SELECT " +
                "suppkey, orderkey, partkey, " +
                "SUM(discount) OVER (PARTITION BY orderkey ORDER BY shipdate, quantity DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a " +
                "FROM lineitem WHERE (partkey = 272 OR partkey = 273) AND suppkey > 50 " +
                "), " +
                "bar AS ( " +
                "SELECT " +
                "suppkey, orderkey, partkey, " +
                "AVG(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate, quantity DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) b " +
                "FROM lineitem WHERE (partkey = 272 OR partkey = 273) AND suppkey > 50 " +
                ")" +
                "SELECT * FROM foo, bar WHERE foo.a = bar.b";

        ExpectedValueProvider<WindowNode.Specification> leftSpecification = specification(
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableList.of(SHIPDATE_ALIAS, QUANTITY_ALIAS),
                ImmutableMap.of(SHIPDATE_ALIAS, SortOrder.ASC_NULLS_LAST, QUANTITY_ALIAS, SortOrder.DESC_NULLS_LAST));

        ExpectedValueProvider<WindowNode.Specification> rightSpecification = specification(
                ImmutableList.of(rOrderkeyAlias),
                ImmutableList.of(rShipdateAlias, rQuantityAlias),
                ImmutableMap.of(rShipdateAlias, SortOrder.ASC_NULLS_LAST, rQuantityAlias, SortOrder.DESC_NULLS_LAST));

        // Too many items in the map to call ImmutableMap.of() :-(
        ImmutableMap.Builder<String, String> leftTableScanBuilder = ImmutableMap.builder();
        leftTableScanBuilder.put(DISCOUNT_ALIAS, "discount");
        leftTableScanBuilder.put(ORDERKEY_ALIAS, "orderkey");
        leftTableScanBuilder.put("PARTKEY", "partkey");
        leftTableScanBuilder.put(SUPPKEY_ALIAS, "suppkey");
        leftTableScanBuilder.put(QUANTITY_ALIAS, "quantity");
        leftTableScanBuilder.put(SHIPDATE_ALIAS, "shipdate");

        PlanMatchPattern leftTableScan = tableScan("lineitem", leftTableScanBuilder.build());

        PlanMatchPattern rightTableScan = tableScan(
                "lineitem",
                ImmutableMap.of(
                        rOrderkeyAlias, "orderkey",
                        "R_PARTKEY", "partkey",
                        "R_SUPPKEY", "suppkey",
                        rQuantityAlias, "quantity",
                        rShipdateAlias, "shipdate"));

        assertUnitPlan(sql,
                anyTree(
                        filter("SUM = AVG",
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        any(
                                                window(leftSpecification, ImmutableMap.of("SUM", functionCall("sum", COMMON_FRAME, ImmutableList.of(DISCOUNT_ALIAS))),
                                                        any(
                                                                leftTableScan))),
                                        any(
                                                window(rightSpecification, ImmutableMap.of("AVG", functionCall("avg", COMMON_FRAME, ImmutableList.of(rQuantityAlias))),
                                                        any(
                                                                rightTableScan)))))));
    }

    @Test
    public void testNotMergeDifferentPartition()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) over (PARTITION BY quantity ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C " +
                "FROM lineitem";

        ExpectedValueProvider<WindowNode.Specification> specificationC = specification(
                ImmutableList.of(QUANTITY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        assertUnitPlan(sql,
                anyTree(
                        window(specificationC, ImmutableList.of(
                                functionCall("sum", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS))),
                                window(specificationA, ImmutableList.of(
                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(DISCOUNT_ALIAS))),
                                        LINEITEM_TABLESCAN_DOQS))));
    }

    @Test
    public void testNotMergeDifferentOrderBy()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY quantity ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C " +
                "FROM lineitem";

       ExpectedValueProvider<WindowNode.Specification> specificationC = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(QUANTITY_ALIAS),
                ImmutableMap.of(QUANTITY_ALIAS, SortOrder.ASC_NULLS_LAST));

        assertUnitPlan(sql,
                anyTree(
                        window(specificationC, ImmutableList.of(
                                functionCall("sum", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS))),
                                window(specificationA, ImmutableList.of(
                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(DISCOUNT_ALIAS))),
                                        LINEITEM_TABLESCAN_DOQS))));
    }

    @Test
    public void testNotMergeDifferentOrdering()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) over (PARTITION BY suppkey ORDER BY orderkey DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "SUM(discount) over (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

       ExpectedValueProvider<WindowNode.Specification> specificationC = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.DESC_NULLS_LAST));

        assertUnitPlan(sql,
                anyTree(
                        window(specificationC, ImmutableList.of(
                                functionCall("sum", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS))),
                                window(specificationA, ImmutableList.of(
                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(EXTENDEDPRICE_ALIAS)),
                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(DISCOUNT_ALIAS))),
                                        LINEITEM_TABLESCAN_DEOQS))));
    }

    @Test
    public void testNotMergeDifferentNullOrdering()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

       ExpectedValueProvider<WindowNode.Specification> specificationC = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_FIRST));

        assertUnitPlan(sql,
                anyTree(
                        window(specificationC, ImmutableList.of(
                                functionCall("sum", COMMON_FRAME, ImmutableList.of(QUANTITY_ALIAS))),
                                window(specificationA, ImmutableList.of(
                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(EXTENDEDPRICE_ALIAS)),
                                        functionCall("sum", COMMON_FRAME, ImmutableList.of(DISCOUNT_ALIAS))),
                                        LINEITEM_TABLESCAN_DEOQS))));
    }

    private void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        LocalQueryRunner queryRunner = getQueryRunner();
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new IterativeOptimizer(new StatsRecorder(), ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new MergeWindows(),
                new PruneUnreferencedOutputs());
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }
}
