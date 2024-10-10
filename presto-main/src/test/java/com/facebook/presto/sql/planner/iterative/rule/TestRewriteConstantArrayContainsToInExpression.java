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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.DELEGATING_ROW_EXPRESSION_OPTIMIZER_ENABLED;
import static com.facebook.presto.SystemSessionProperties.REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestRewriteConstantArrayContainsToInExpression
        extends BaseRuleTest
{
    @DataProvider(name = "delegating-row-expression-optimizer-enabled")
    public Object[][] delegatingDataProvider()
    {
        return new Object[][] {
                {true},
                {false},
        };
    }

    @Test(dataProvider = "delegating-row-expression-optimizer-enabled")
    public void testNoNull(boolean enableDelegatingRowExpressionOptimizer)
    {
        tester().assertThat(
                ImmutableSet.<Rule<?>>builder().addAll(new SimplifyRowExpressions(getMetadata(), getExpressionManager()).rules()).addAll(
                        new RewriteConstantArrayContainsToInExpression(getFunctionManager()).rules()).build())
                .setSystemProperty(REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION, "true")
                .setSystemProperty(DELEGATING_ROW_EXPRESSION_OPTIMIZER_ENABLED, Boolean.toString(enableDelegatingRowExpressionOptimizer))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BOOLEAN);
                    VariableReferenceExpression b = p.variable("b");
                    return p.project(
                            assignment(a, p.rowExpression("contains(array[1, 2, 3], b)")),
                            p.values(b));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", expression("b IN (1, 2, 3)")),
                                values("b")));
    }

    @Test(dataProvider = "delegating-row-expression-optimizer-enabled")
    public void testDoesNotFireForNestedArray(boolean enableDelegatingRowExpressionOptimizer)
    {
        tester().assertThat(new RewriteConstantArrayContainsToInExpression(getFunctionManager()).projectRowExpressionRewriteRule())
                .setSystemProperty(REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION, "true")
                .setSystemProperty(DELEGATING_ROW_EXPRESSION_OPTIMIZER_ENABLED, Boolean.toString(enableDelegatingRowExpressionOptimizer))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BOOLEAN);
                    VariableReferenceExpression b = p.variable("b", new ArrayType(BIGINT));
                    return p.project(
                            assignment(a, p.rowExpression("contains(array[array[1, 2], array[3]], b)")),
                            p.values(b));
                })
                .doesNotFire();
    }

    @Test(dataProvider = "delegating-row-expression-optimizer-enabled")
    public void testDoesNotFireForNull(boolean enableDelegatingRowExpressionOptimizer)
    {
        tester().assertThat(new RewriteConstantArrayContainsToInExpression(getFunctionManager()).projectRowExpressionRewriteRule())
                .setSystemProperty(REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BOOLEAN);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    return p.project(
                            assignment(a, p.rowExpression("contains(cast(null as array<bigint>), b)")),
                            p.values(b));
                })
                .doesNotFire();
    }

    @Test(dataProvider = "delegating-row-expression-optimizer-enabled")
    public void testDoesNotFireForEmpty(boolean enableDelegatingRowExpressionOptimizer)
    {
        tester().assertThat(new RewriteConstantArrayContainsToInExpression(getFunctionManager()).projectRowExpressionRewriteRule())
                .setSystemProperty(REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BOOLEAN);
                    VariableReferenceExpression b = p.variable("b", new ArrayType(BIGINT));
                    return p.project(
                            assignment(a, p.rowExpression("contains(array[], b)")),
                            p.values(b));
                })
                .doesNotFire();
    }

    @Test(dataProvider = "delegating-row-expression-optimizer-enabled")
    public void testNotFire(boolean enableDelegatingRowExpressionOptimizer)
    {
        tester().assertThat(
                ImmutableSet.<Rule<?>>builder().addAll(new SimplifyRowExpressions(getMetadata(), getExpressionManager()).rules()).addAll(
                        new RewriteConstantArrayContainsToInExpression(getFunctionManager()).rules()).build())
                .setSystemProperty(REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BOOLEAN);
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    return p.project(
                            assignment(a, p.rowExpression("contains(array[1, 2, c], b)")),
                            p.values(b, c));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", expression("contains(array[1, 2, c], b)")),
                                values("b", "c")));
    }

    @Test(dataProvider = "delegating-row-expression-optimizer-enabled")
    public void testWithNull(boolean enableDelegatingRowExpressionOptimizer)
    {
        tester().assertThat(
                ImmutableSet.<Rule<?>>builder().addAll(new SimplifyRowExpressions(getMetadata(), getExpressionManager()).rules()).addAll(
                        new RewriteConstantArrayContainsToInExpression(getFunctionManager()).rules()).build())
                .setSystemProperty(REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BOOLEAN);
                    VariableReferenceExpression b = p.variable("b");
                    return p.project(
                            assignment(a, p.rowExpression("contains(array[1, 2, 3, null], b)")),
                            p.values(b));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", expression("b IN (1, 2, 3, null)")),
                                values("b")));
    }

    @Test(dataProvider = "delegating-row-expression-optimizer-enabled")
    public void testLambda(boolean enableDelegatingRowExpressionOptimizer)
    {
        tester().assertThat(
                ImmutableSet.<Rule<?>>builder().addAll(new SimplifyRowExpressions(getMetadata(), getExpressionManager()).rules()).addAll(
                        new RewriteConstantArrayContainsToInExpression(getFunctionManager()).rules()).build())
                .setSystemProperty(REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BOOLEAN);
                    VariableReferenceExpression b = p.variable("b", new ArrayType(BIGINT));
                    return p.project(
                            assignment(a, p.rowExpression("any_match(b, x -> contains(array[1, 2, 3, null], x))")),
                            p.values(b));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", expression("any_match(b, x -> x IN (1, 2, 3, null))")),
                                values("b")));
    }
}
