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
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.topN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.LAST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;

public class TestReplaceConstantVariableReferencesWithConstants
        extends BasePlanTest
{
    private Session enableOptimization()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION, "true")
                .build();
    }

    @Test
    public void testAggregation()
    {
        assertPlan("select orderkey, orderpriority, avg(totalprice) from orders where orderpriority='3-MEDIUM' group by 1, 2",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "expr_12", "avg"),
                        project(
                                ImmutableMap.of("expr_12", expression("'3-MEDIUM'")),
                                aggregation(
                                        ImmutableMap.of("avg", functionCall("avg", ImmutableList.of("totalprice"))),
                                        anyTree(
                                                filter(
                                                        "orderpriority = '3-MEDIUM'",
                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "orderpriority", "orderpriority", "totalprice", "totalprice"))))))));
    }

    @Test
    public void testUnnest()
    {
        assertPlan("select orderkey, orderpriority, idx from orders cross join unnest(array[1, 2]) t(idx) where orderpriority='3-MEDIUM'",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "expr_9", "field"),
                        project(
                                ImmutableMap.of("expr_9", expression("'3-MEDIUM'")),
                                unnest(
                                        ImmutableMap.of("expr", ImmutableList.of("field")),
                                        project(
                                                ImmutableMap.of("expr", expression("array[1, 2]")),
                                                filter(
                                                        "orderpriority = '3-MEDIUM'",
                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "orderpriority", "orderpriority"))))))));
    }

    @Test
    public void testInnerJoin()
    {
        assertPlan("select o.orderkey, o.orderpriority, l.tax from lineitem l join orders o on o.orderkey = l.orderkey where o.orderpriority='3-MEDIUM'",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey_0", "expr_14", "tax"),
                        project(
                                ImmutableMap.of("expr_14", expression("'3-MEDIUM'")),
                                join(
                                        JoinType.INNER,
                                        ImmutableList.of(equiJoinClause("orderkey", "orderkey_0")),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "tax", "tax"))),
                                        anyTree(
                                                filter(
                                                        "orderpriority = '3-MEDIUM'",
                                                        tableScan("orders", ImmutableMap.of("orderkey_0", "orderkey", "orderpriority", "orderpriority"))))))));
    }

    @Test
    public void testLeftJoinNotTrigger()
    {
        assertPlan("select o.orderkey, o.orderpriority, l.tax from lineitem l left join (select orderkey, orderpriority from orders where orderpriority='3-MEDIUM') o on o.orderkey = l.orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey_0", "expr_9", "tax"),
                        join(
                                JoinType.LEFT,
                                ImmutableList.of(equiJoinClause("orderkey", "orderkey_0")),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "tax", "tax"))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("expr_9", expression("'3-MEDIUM'")),
                                                filter(
                                                        "orderpriority = '3-MEDIUM'",
                                                        tableScan("orders", ImmutableMap.of("orderkey_0", "orderkey", "orderpriority", "orderpriority"))))))));
    }

    @Test
    public void testLeftJoinTrigger()
    {
        assertPlan("select o.orderkey, l.linestatus, l.tax from (select tax, linestatus, orderkey from lineitem where linestatus ='O') l left join orders o on o.orderkey = l.orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey_0", "expr_26", "tax"),
                        project(
                                ImmutableMap.of("expr_26", expression("'O'")),
                                join(
                                        JoinType.LEFT,
                                        ImmutableList.of(equiJoinClause("orderkey", "orderkey_0")),
                                        anyTree(
                                                filter(
                                                        "linestatus = 'O'",
                                                        tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "tax", "tax", "linestatus", "linestatus")))),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("orderkey_0", "orderkey")))))));
    }

    @Test
    public void testSemiJoin()
    {
        assertPlan("select orderpriority, orderkey from orders where orderpriority='3-MEDIUM' and orderkey in (select orderkey from lineitem)",
                enableOptimization(),
                output(
                        ImmutableList.of("expr_15", "orderkey"),
                        project(
                                ImmutableMap.of("expr_15", expression("'3-MEDIUM'")),
                                filter(
                                        "expr_8",
                                        project(
                                                semiJoin(
                                                        "orderkey",
                                                        "orderkey_1",
                                                        "expr_8",
                                                        project(
                                                                filter(
                                                                        "orderpriority = '3-MEDIUM'",
                                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "orderpriority", "orderpriority")))),
                                                        anyTree(
                                                                project(
                                                                        tableScan("lineitem", ImmutableMap.of("orderkey_1", "orderkey"))))))))));
    }

    @Test
    public void testSimpleFilter()
    {
        assertPlan("select orderkey, orderpriority from orders where orderpriority='3-MEDIUM'",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "expr_6"),
                        project(
                                ImmutableMap.of("expr_6", expression("'3-MEDIUM'")),
                                filter(
                                        "orderpriority = '3-MEDIUM'",
                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "orderpriority", "orderpriority"))))));
    }

    @Test
    public void testFilterOnSameVariable()
    {
        assertPlan("select orderkey, orderpriority from orders where orderpriority='3-MEDIUM' and orderpriority='5-HIGH'",
                enableOptimization(),
                output(
                        values("orderkey", "orderpriority")));
    }

    @Test
    public void testJoinWithFilter()
    {
        assertPlan("with t1 as (select orderkey, orderstatus from orders where orderkey = 10) select l.orderkey, partkey, orderstatus from t1 join lineitem l on t1.orderkey = l.orderkey where partkey in (select suppkey from lineitem)",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey_9", "partkey", "orderstatus"),
                        project(
                                ImmutableMap.of("orderkey_9", expression("10")),
                                filter(
                                        "expr_37",
                                        project(
                                                semiJoin("partkey", "suppkey_17", "expr_37",
                                                        project(
                                                                join(JoinType.INNER,
                                                                        ImmutableList.of(),
                                                                        anyTree(
                                                                                filter("orderkey = 10",
                                                                                        tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus", "orderkey", "orderkey")))),
                                                                        anyTree(
                                                                                filter(
                                                                                        "orderkey_9 = 10",
                                                                                        tableScan("lineitem", ImmutableMap.of("orderkey_9", "orderkey", "partkey", "partkey")))))),
                                                        anyTree(
                                                                tableScan("lineitem", ImmutableMap.of("suppkey_17", "suppkey")))))))));
    }

    @Test
    public void testConstantRowExpression()
    {
        assertPlan("select orderkey+1 as nk from lineitem where orderkey=1",
                enableOptimization(),
                output(
                        project(
                                ImmutableMap.of("expr", expression("2")),
                                filter(
                                        "orderkey=1",
                                        tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey"))))));
    }

    @Test
    public void testSort()
    {
        assertPlan("select orderkey, orderpriority from orders where orderpriority='3-MEDIUM' order by orderpriority",
                enableOptimization(),
                output(
                        project(
                                ImmutableMap.of("orderkey", expression("orderkey"), "orderpriority", expression("'3-MEDIUM'")),
                                filter(
                                        "orderpriority='3-MEDIUM'",
                                        tableScan("orders", ImmutableMap.of("orderpriority", "orderpriority", "orderkey", "orderkey"))))));
    }

    @Test
    public void testSortOnMultipleKey()
    {
        ImmutableList<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("orderkey", ASCENDING, LAST));
        assertPlan("select orderkey, orderpriority from orders where orderpriority='3-MEDIUM' order by orderpriority, orderkey",
                enableOptimization(),
                output(
                        project(
                                ImmutableMap.of("orderkey", expression("orderkey"), "orderpriority", expression("'3-MEDIUM'")),
                                anyTree(
                                        sort(orderBy,
                                                anyTree(
                                                        project(
                                                                ImmutableMap.of("orderkey", expression("orderkey")),
                                                                filter(
                                                                        "orderpriority='3-MEDIUM'",
                                                                        tableScan("orders", ImmutableMap.of("orderpriority", "orderpriority", "orderkey", "orderkey"))))))))));
    }

    @Test
    public void testTopN()
    {
        assertPlan("select orderkey, orderpriority from orders where orderpriority='3-MEDIUM' order by orderpriority limit 10",
                enableOptimization(),
                output(
                        project(
                                ImmutableMap.of("orderkey", expression("orderkey"), "orderpriority", expression("'3-MEDIUM'")),
                                limit(
                                        10,
                                        anyTree(
                                                filter(
                                                        "orderpriority='3-MEDIUM'",
                                                        tableScan("orders", ImmutableMap.of("orderpriority", "orderpriority", "orderkey", "orderkey"))))))));
    }

    @Test
    public void testTopNSortOnMultipleKey()
    {
        ImmutableList<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("orderkey", ASCENDING, LAST));
        assertPlan("select orderkey, orderpriority from orders where orderpriority='3-MEDIUM' order by orderpriority, orderkey limit 10",
                enableOptimization(),
                output(
                        project(
                                ImmutableMap.of("orderkey", expression("orderkey"), "orderpriority", expression("'3-MEDIUM'")),
                                topN(10, orderBy,
                                        anyTree(
                                                topN(
                                                        10,
                                                        orderBy,
                                                        project(
                                                                ImmutableMap.of("orderkey", expression("orderkey")),
                                                                filter(
                                                                        "orderpriority='3-MEDIUM'",
                                                                        tableScan("orders", ImmutableMap.of("orderpriority", "orderpriority", "orderkey", "orderkey"))))))))));
    }

    @Test
    public void testUnionAllWithSameConstant()
    {
        assertPlan("select orderkey, price, count(*) from (select orderkey, extendedprice as price from lineitem where orderkey=5 union all select orderkey, totalprice as price from orders where orderkey=5) group by orderkey, price",
                enableOptimization(),
                output(
                        project(
                                ImmutableMap.of("orderkey_11", expression("5")),
                                aggregation(
                                        ImmutableMap.of("count", functionCall("count", ImmutableList.of("count_29"))),
                                        exchange(
                                                anyTree(
                                                        aggregation(
                                                                ImmutableMap.of("count_29", functionCall("count", ImmutableList.of())),
                                                                project(
                                                                        filter(
                                                                                "orderkey = 5",
                                                                                tableScan("lineitem", ImmutableMap.of("extendedprice", "extendedprice", "orderkey", "orderkey")))))),
                                                anyTree(
                                                        aggregation(
                                                                ImmutableMap.of("count_29", functionCall("count", ImmutableList.of())),
                                                                project(
                                                                        filter(
                                                                                "orderkey_4 = 5",
                                                                                tableScan("orders", ImmutableMap.of("orderkey_4", "orderkey", "totalprice", "totalprice")))))))))));
    }

    @Test
    public void testUnionAllWithDifferentConstant()
    {
        assertPlan("select orderkey, price, count(*) from (select orderkey, extendedprice as price from lineitem where orderkey=5 union all select orderkey, totalprice as price from orders where orderkey=2) group by orderkey, price",
                enableOptimization(),
                output(
                        project(
                                ImmutableMap.of("orderkey_11", expression("orderkey_11")),
                                aggregation(
                                        ImmutableMap.of("count", functionCall("count", ImmutableList.of("count_29"))),
                                        exchange(
                                                project(
                                                        project(
                                                                ImmutableMap.of("orderkey_11", expression("orderkey")),
                                                                aggregation(
                                                                        ImmutableMap.of("count_29", functionCall("count", ImmutableList.of())),
                                                                        project(
                                                                                project(
                                                                                        ImmutableMap.of("orderkey", expression("5")),
                                                                                        filter(
                                                                                                "orderkey = 5",
                                                                                                tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "extendedprice", "extendedprice")))))))),
                                                project(
                                                        project(
                                                                ImmutableMap.of("orderkey_11", expression("orderkey_4")),
                                                                aggregation(
                                                                        ImmutableMap.of("count_29", functionCall("count", ImmutableList.of())),
                                                                        project(
                                                                                project(
                                                                                        ImmutableMap.of("orderkey_4", expression("2")),
                                                                                        filter(
                                                                                                "orderkey_4 = 2",
                                                                                                tableScan("orders", ImmutableMap.of("orderkey_4", "orderkey", "totalprice", "totalprice")))))))))))));
    }

    @Test
    public void testUnionAllWithOneConstant()
    {
        assertPlan("select orderkey, price, count(*) from (select orderkey, extendedprice as price from lineitem where orderkey=5 union all select orderkey, totalprice as price from orders) group by orderkey, price",
                enableOptimization(),
                output(
                        project(
                                ImmutableMap.of("orderkey_11", expression("orderkey_11")),
                                aggregation(
                                        ImmutableMap.of("count", functionCall("count", ImmutableList.of("count_29"))),
                                        exchange(
                                                project(
                                                        project(
                                                                ImmutableMap.of("orderkey_11", expression("orderkey")),
                                                                aggregation(
                                                                        ImmutableMap.of("count_29", functionCall("count", ImmutableList.of())),
                                                                        project(
                                                                                project(
                                                                                        ImmutableMap.of("orderkey", expression("5")),
                                                                                        filter(
                                                                                                "orderkey = 5",
                                                                                                tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "extendedprice", "extendedprice")))))))),
                                                project(
                                                        project(
                                                                ImmutableMap.of("orderkey_11", expression("orderkey_4")),
                                                                aggregation(
                                                                        ImmutableMap.of("count_29", functionCall("count", ImmutableList.of())),
                                                                        project(
                                                                                tableScan("orders", ImmutableMap.of("orderkey_4", "orderkey", "totalprice", "totalprice")))))))))));
    }

    @Test
    public void testExtractConstantFromFilter()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    VariableReferenceExpression count = planBuilder.variable("cnt");
                    return planBuilder.aggregation(
                            aggregationBuilder -> aggregationBuilder
                                    .source(
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("key1= 3"),
                                                    planBuilder.values(key1, key2)))
                                    .singleGroupingSet(key1, key2)
                                    .addAggregation(count, planBuilder.rowExpression("count()")));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("cnt", functionCall("count", ImmutableList.of())),
                                project(
                                        ImmutableMap.of("key1", expression("3")),
                                        filter(
                                                "key1=3",
                                                values("key1", "key2")))));
    }

    @Test
    public void testJoinPlanChange()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    return planBuilder.join(
                            JoinType.INNER,
                            planBuilder.filter(
                                    planBuilder.rowExpression("key1= 3"),
                                    planBuilder.values(key1)),
                            planBuilder.filter(
                                    planBuilder.rowExpression("key2= 5"),
                                    planBuilder.values(key2)));
                })
                .matches(
                        join(
                                project(
                                        ImmutableMap.of("key1", expression("3")),
                                        filter(
                                                "key1=3",
                                                values("key1"))),
                                project(
                                        ImmutableMap.of("key2", expression("5")),
                                        filter(
                                                "key2=5",
                                                values("key2")))));
    }

    @Test
    public void testUnionPlanChange()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression input1Source1 = planBuilder.variable("input1_source1", INTEGER);
                    VariableReferenceExpression input2Source1 = planBuilder.variable("input2_source1", INTEGER);
                    VariableReferenceExpression input1Source2 = planBuilder.variable("input1_source2", INTEGER);
                    VariableReferenceExpression input2Source2 = planBuilder.variable("input2_source2", INTEGER);
                    VariableReferenceExpression output1 = planBuilder.variable("output1", INTEGER);
                    VariableReferenceExpression output2 = planBuilder.variable("output2", INTEGER);

                    return
                            planBuilder.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder().putAll(output1, input1Source1, input1Source2)
                                            .putAll(output2, input2Source1, input2Source2).build(),
                                    ImmutableList.of(
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("input1_source1 = 3"),
                                                    planBuilder.values(input1Source1, input2Source1)),
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("input1_source2 = 3"),
                                                    planBuilder.values(input1Source2, input2Source2))));
                })
                .matches(

                        union(
                                project(
                                        ImmutableMap.of("input1_source1", expression("3"), "input2_source1", expression("input2_source1")),
                                        filter(
                                                "input1_source1=3",
                                                values("input1_source1", "input2_source1"))),
                                project(
                                        ImmutableMap.of("input1_source2", expression("3"), "input2_source2", expression("input2_source2")),
                                        filter(
                                                "input1_source2=3",
                                                values("input1_source2", "input2_source2")))));
    }

    // Do not extract constant variable when having conflicting filters
    @Test
    public void testConflictFilter()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    VariableReferenceExpression count = planBuilder.variable("cnt");
                    return planBuilder.aggregation(
                            aggregationBuilder -> aggregationBuilder
                                    .source(
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("key1=2"),
                                                    planBuilder.filter(
                                                            planBuilder.rowExpression("key1= 3"),
                                                            planBuilder.values(key1, key2))))
                                    .singleGroupingSet(key1, key2)
                                    .addAggregation(count, planBuilder.rowExpression("count()")));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("cnt", functionCall("count", ImmutableList.of())),
                                filter(
                                        "key1=2",
                                        filter(
                                                "key1=3",
                                                values("key1", "key2")))));
    }

    @Test
    public void testConflictFilterConjunct()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    VariableReferenceExpression count = planBuilder.variable("cnt");
                    return planBuilder.aggregation(
                            aggregationBuilder -> aggregationBuilder
                                    .source(
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("key1=2 and key2=5"),
                                                    planBuilder.filter(
                                                            planBuilder.rowExpression("key1= 3"),
                                                            planBuilder.values(key1, key2))))
                                    .singleGroupingSet(key1, key2)
                                    .addAggregation(count, planBuilder.rowExpression("count()")));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("cnt", functionCall("count", ImmutableList.of())),
                                filter(
                                        "key1=2 and key2=5",
                                        filter(
                                                "key1=3",
                                                values("key1", "key2")))));
    }

    @Test
    public void testNonConflictFilter()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    VariableReferenceExpression count = planBuilder.variable("cnt");
                    return planBuilder.aggregation(
                            aggregationBuilder -> aggregationBuilder
                                    .source(
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("key1=3"),
                                                    planBuilder.filter(
                                                            planBuilder.rowExpression("key1= 3"),
                                                            planBuilder.values(key1, key2))))
                                    .singleGroupingSet(key1, key2)
                                    .addAggregation(count, planBuilder.rowExpression("count()")));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("cnt", functionCall("count", ImmutableList.of())),
                                project(
                                        ImmutableMap.of("key1", expression("3")),
                                        filter(
                                                "3=3",
                                                filter(
                                                        "key1=3",
                                                        values("key1", "key2"))))));
    }

    @Test
    public void testNonConflictFilterConjunct()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    VariableReferenceExpression count = planBuilder.variable("cnt");
                    return planBuilder.aggregation(
                            aggregationBuilder -> aggregationBuilder
                                    .source(
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("key1=3 and key2=5"),
                                                    planBuilder.filter(
                                                            planBuilder.rowExpression("key1= 3"),
                                                            planBuilder.values(key1, key2))))
                                    .singleGroupingSet(key1, key2)
                                    .addAggregation(count, planBuilder.rowExpression("count()")));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("cnt", functionCall("count", ImmutableList.of())),
                                project(
                                        ImmutableMap.of("key1", expression("3"), "key2", expression("5")),
                                        filter(
                                                "3=3 and key2=5",
                                                filter(
                                                        "key1=3",
                                                        values("key1", "key2"))))));
    }

    @Test
    public void testFilterOnExpression()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    VariableReferenceExpression count = planBuilder.variable("cnt");
                    return planBuilder.aggregation(
                            aggregationBuilder -> aggregationBuilder
                                    .source(
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("key2=key1+2"),
                                                    planBuilder.filter(
                                                            planBuilder.rowExpression("key1= 3"),
                                                            planBuilder.values(key1, key2))))
                                    .singleGroupingSet(key1, key2)
                                    .addAggregation(count, planBuilder.rowExpression("count()")));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("cnt", functionCall("count", ImmutableList.of())),
                                project(
                                        ImmutableMap.of("key1", expression("3"), "key2", expression("key2")),
                                        filter(
                                                "key2=3+2",
                                                filter(
                                                        "key1=3",
                                                        values("key1", "key2"))))));
    }

    @Test
    public void testFilterAndProjectOnExpression()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    VariableReferenceExpression count = planBuilder.variable("cnt");
                    return planBuilder.aggregation(
                            aggregationBuilder -> aggregationBuilder
                                    .source(
                                            planBuilder.project(
                                                    assignment(key2, planBuilder.rowExpression("key1+2"), key1, planBuilder.rowExpression("key1")),
                                                    planBuilder.filter(
                                                            planBuilder.rowExpression("key1= 3"),
                                                            planBuilder.values(key1, key2))))
                                    .singleGroupingSet(key1, key2)
                                    .addAggregation(count, planBuilder.rowExpression("count()")));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("cnt", functionCall("count", ImmutableList.of())),
                                project(
                                        ImmutableMap.of("key1", expression("3"), "key2", expression("key2")),
                                        project(
                                                ImmutableMap.of("key2", expression("3+2"), "key1", expression("3")),
                                                filter(
                                                        "key1=3",
                                                        values("key1", "key2"))))));
    }

    @Test
    public void testConflictFilterAndProject()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    VariableReferenceExpression count = planBuilder.variable("cnt");
                    return planBuilder.aggregation(
                            aggregationBuilder -> aggregationBuilder
                                    .source(
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("key1= 3"),
                                                    planBuilder.project(
                                                            assignment(key1, planBuilder.rowExpression("2"), key2, planBuilder.rowExpression("key2")),
                                                            planBuilder.values(key1, key2))))
                                    .singleGroupingSet(key1, key2)
                                    .addAggregation(count, planBuilder.rowExpression("count()")));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("cnt", functionCall("count", ImmutableList.of())),
                                filter(
                                        "key1=3",
                                        project(
                                                ImmutableMap.of("key1", expression("2")),
                                                values("key1", "key2")))));
    }

    @Test
    public void testExtractConstantFromProject()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    VariableReferenceExpression count = planBuilder.variable("cnt");
                    return planBuilder.aggregation(
                            aggregationBuilder -> aggregationBuilder
                                    .source(
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("key2 <> 3"),
                                                    planBuilder.project(
                                                            assignment(key1, planBuilder.rowExpression("3"), key2, planBuilder.rowExpression("key2")),
                                                            planBuilder.values(key1, key2))))
                                    .singleGroupingSet(key1, key2)
                                    .addAggregation(count, planBuilder.rowExpression("count()")));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("cnt", functionCall("count", ImmutableList.of())),
                                project(
                                        ImmutableMap.of("key1", expression("3")),
                                        filter(
                                                "key2 <> 3",
                                                project(
                                                        ImmutableMap.of("key1", expression("3"), "key2", expression("key2")),
                                                        values("key1", "key2"))))));
    }

    // If project get conflict constant, use the latest one
    @Test
    public void testConflictConstantFromProject()
    {
        RuleTester tester = new RuleTester();
        tester.assertThat(new ReplaceConstantVariableReferencesWithConstants(createTestFunctionAndTypeManager()))
                .on(planBuilder ->
                {
                    VariableReferenceExpression key1 = planBuilder.variable("key1", INTEGER);
                    VariableReferenceExpression key2 = planBuilder.variable("key2", INTEGER);
                    VariableReferenceExpression count = planBuilder.variable("cnt");
                    return planBuilder.aggregation(
                            aggregationBuilder -> aggregationBuilder
                                    .source(
                                            planBuilder.filter(
                                                    planBuilder.rowExpression("key2 <> 3"),
                                                    planBuilder.project(
                                                            assignment(key1, planBuilder.rowExpression("5"), key2, planBuilder.rowExpression("key2")),
                                                            planBuilder.project(
                                                                    assignment(key1, planBuilder.rowExpression("3"), key2, planBuilder.rowExpression("key2")),
                                                                    planBuilder.values(key1, key2)))))
                                    .singleGroupingSet(key1, key2)
                                    .addAggregation(count, planBuilder.rowExpression("count()")));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("cnt", functionCall("count", ImmutableList.of())),
                                project(
                                        ImmutableMap.of("key1", expression("5")),
                                        filter(
                                                "key2 <> 3",
                                                project(
                                                        ImmutableMap.of("key1", expression("5"), "key2", expression("key2")),
                                                        project(
                                                                ImmutableMap.of("key1", expression("3"), "key2", expression("key2")),
                                                                values("key1", "key2")))))));
    }
}
