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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.lateral;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestTransformExistsApplyToLateralJoin
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new TransformExistsApplyToLateralNode(tester().getMetadata().getFunctionRegistry()))
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();

        tester().assertThat(new TransformExistsApplyToLateralNode(tester().getMetadata().getFunctionRegistry()))
                .on(p ->
                        p.lateral(
                                ImmutableList.of(p.symbol("a")),
                                p.values(p.symbol("a")),
                                p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void testRewrite()
    {
        tester().assertThat(new TransformExistsApplyToLateralNode(tester().getMetadata().getFunctionRegistry()))
                .on(p ->
                        p.apply(
                                Assignments.of(p.symbol("b", BOOLEAN), expression("EXISTS(SELECT TRUE)")),
                                ImmutableList.of(),
                                p.values(),
                                p.values()))
                .matches(lateral(
                        ImmutableList.of(),
                        values(ImmutableMap.of()),
                        project(
                                ImmutableMap.of("b", PlanMatchPattern.expression("(\"count_expr\" > CAST(0 AS bigint))")),
                                aggregation(ImmutableMap.of("count_expr", functionCall("count", ImmutableList.of())),
                                        values()))));
    }

    @Test
    public void testRewritesToLimit()
    {
        tester().assertThat(new TransformExistsApplyToLateralNode(tester().getMetadata().getFunctionRegistry()))
                .on(p ->
                        p.apply(
                                Assignments.of(p.symbol("b", BOOLEAN), expression("EXISTS(SELECT TRUE)")),
                                ImmutableList.of(p.symbol("corr")),
                                p.values(p.symbol("corr")),
                                p.project(Assignments.of(),
                                        p.filter(
                                                expression("corr = column"),
                                                p.values(p.symbol("column"))))))
                .matches(
                        project(ImmutableMap.of("b", PlanMatchPattern.expression("COALESCE(subquerytrue, false)")),
                                lateral(
                                        ImmutableList.of("corr"),
                                        values("corr"),
                                        project(
                                                ImmutableMap.of("subquerytrue", PlanMatchPattern.expression("true")),
                                                limit(1,
                                                        project(ImmutableMap.of(),
                                                                node(FilterNode.class,
                                                                        values("column"))))))));
    }
}
