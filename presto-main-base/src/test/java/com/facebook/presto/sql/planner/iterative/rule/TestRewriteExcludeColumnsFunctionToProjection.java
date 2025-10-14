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

import com.facebook.presto.operator.table.ExcludeColumns.ExcludeColumnsFunctionHandle;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRewriteExcludeColumnsFunctionToProjection
        extends BaseRuleTest
{
    @Test
    public void rewriteExcludeColumnsFunction()
    {
        tester().assertThat(new RewriteExcludeColumnsFunctionToProjection())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BOOLEAN);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression c = p.variable("c", SMALLINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression y = p.variable("y", SMALLINT);
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("exclude_columns")
                                    .properOutputs(x, y)
                                    .pruneWhenEmpty()
                                    .requiredSymbols(ImmutableList.of(ImmutableList.of(b, c)))
                                    .connectorHandle(new ExcludeColumnsFunctionHandle())
                                    .source(p.values(a, b, c)));
                })
                .matches(PlanMatchPattern.strictProject(
                        ImmutableMap.of(
                                "x", expression("b"),
                                "y", expression("c")),
                        values("a", "b", "c")));
    }

    @Test
    public void doNotRewriteOtherFunction()
    {
        tester().assertThat(new RewriteExcludeColumnsFunctionToProjection())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BOOLEAN);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression c = p.variable("c", SMALLINT);
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("testing_function")
                                    .requiredSymbols(ImmutableList.of(ImmutableList.of(b, c)))
                                    .source(p.values(a, b, c)));
                }).doesNotFire();
    }
}
