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

import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.OptimizerAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_VALUES_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class TestValuesJoinOptimizer
        extends BaseRuleTest
{
    private OptimizerAssert assertOptimizer()
    {
        RuleTester tester = tester();
        return tester.assertThat(new ValuesJoinOptimizer(tester.getMetadata()))
                .setSystemProperty(OPTIMIZE_VALUES_JOIN, "true");
    }

    private OptimizerAssert assertOptimizerDisabled()
    {
        RuleTester tester = tester();
        return tester.assertThat(new ValuesJoinOptimizer(tester.getMetadata()))
                .setSystemProperty(OPTIMIZE_VALUES_JOIN, "false");
    }

    @Test
    public void testSingleRowInnerJoinReplacedWithFilterProject()
    {
        // Single-row VALUES INNER JOIN should be replaced with Filter + Project (no JoinNode)
        assertOptimizer()
                .on(p -> {
                    VariableReferenceExpression nationkey = p.variable("nationkey", BIGINT);
                    VariableReferenceExpression name = p.variable("name", VARCHAR);
                    VariableReferenceExpression regionkey = p.variable("regionkey", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);

                    PlanNode left = p.values(nationkey, name, regionkey);
                    ValuesNode right = p.values(
                            ImmutableList.of(x),
                            ImmutableList.of(ImmutableList.of(constant(1L, BIGINT))));

                    return p.output(
                            ImmutableList.of("nationkey", "name", "x"),
                            ImmutableList.of(nationkey, name, x),
                            p.join(INNER, left, right, new EquiJoinClause(regionkey, x)));
                })
                .matches(
                        output(
                                project(
                                        filter(
                                                values("nationkey", "name", "regionkey")))));
    }

    @Test
    public void testSingleRowLeftJoinReplacedWithIfProjection()
    {
        // Single-row VALUES LEFT JOIN should be replaced with IF-based projection
        assertOptimizer()
                .on(p -> {
                    VariableReferenceExpression nationkey = p.variable("nationkey", BIGINT);
                    VariableReferenceExpression regionkey = p.variable("regionkey", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);

                    PlanNode left = p.values(nationkey, regionkey);
                    ValuesNode right = p.values(
                            ImmutableList.of(x),
                            ImmutableList.of(ImmutableList.of(constant(1L, BIGINT))));

                    return p.output(
                            ImmutableList.of("nationkey", "x"),
                            ImmutableList.of(nationkey, x),
                            p.join(LEFT, left, right, new EquiJoinClause(regionkey, x)));
                })
                .matches(
                        output(
                                project(
                        values("nationkey", "regionkey"))));
    }

    @Test
    public void testOptimizationSkippedWhenDisabled()
    {
        // When disabled, the JoinNode should remain
        assertOptimizerDisabled()
                .on(p -> {
                    VariableReferenceExpression nationkey = p.variable("nationkey", BIGINT);
                    VariableReferenceExpression regionkey = p.variable("regionkey", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);

                    PlanNode left = p.values(nationkey, regionkey);
                    ValuesNode right = p.values(
                            ImmutableList.of(x),
                            ImmutableList.of(ImmutableList.of(constant(1L, BIGINT))));

                    return p.output(
                            ImmutableList.of("nationkey", "x"),
                            ImmutableList.of(nationkey, x),
                            p.join(INNER, left, right, new EquiJoinClause(regionkey, x)));
                })
                .matches(
                        output(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("regionkey", "x")),
                                        values("nationkey", "regionkey"),
                                        values("x"))));
    }

    @Test
    public void testLargeValuesSkipped()
    {
        // Optimization should be skipped for VALUES with > 1000 rows
        assertOptimizer()
                .on(p -> {
                    VariableReferenceExpression key = p.variable("key", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);

                    PlanNode left = p.values(key);

                    ImmutableList.Builder<List<com.facebook.presto.spi.relation.RowExpression>> rowsBuilder = ImmutableList.builder();
                    for (int i = 0; i < 1001; i++) {
                        rowsBuilder.add(ImmutableList.of(constant((long) i, BIGINT)));
                    }

                    ValuesNode right = p.values(ImmutableList.of(x), rowsBuilder.build());

                    return p.output(
                            ImmutableList.of("key", "x"),
                            ImmutableList.of(key, x),
                            p.join(INNER, left, right, new EquiJoinClause(key, x)));
                })
                .matches(
                        output(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("key", "x")),
                                        values("key"),
                                        values("x"))));
    }
}
