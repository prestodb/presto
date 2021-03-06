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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.lateral;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestTransformExistsApplyToLateralJoin
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new TransformExistsApplyToLateralNode(tester().getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.values(p.variable("a")))
                .doesNotFire();

        tester().assertThat(new TransformExistsApplyToLateralNode(tester().getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                        p.lateral(
                                ImmutableList.of(p.variable("a")),
                                p.values(p.variable("a")),
                                p.values(p.variable("a"))))
                .doesNotFire();
    }

    @Test
    public void testRewrite()
    {
        tester().assertThat(new TransformExistsApplyToLateralNode(tester().getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                        p.apply(
                                assignment(p.variable("b", BOOLEAN), expression("EXISTS(SELECT TRUE)")),
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
        tester().assertThat(new TransformExistsApplyToLateralNode(tester().getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                        p.apply(
                                assignment(p.variable("b", BOOLEAN), expression("EXISTS(SELECT TRUE)")),
                                ImmutableList.of(p.variable("corr")),
                                p.values(p.variable("corr")),
                                p.project(Assignments.of(),
                                        p.filter(
                                                expression("corr = column"),
                                                p.values(p.variable("column"))))))
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
