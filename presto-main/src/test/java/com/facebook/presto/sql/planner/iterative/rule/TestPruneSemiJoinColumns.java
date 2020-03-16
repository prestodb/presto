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

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestPruneSemiJoinColumns
        extends BaseRuleTest
{
    @Test
    public void testSemiJoinNotNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, variable -> variable.getName().equals("leftValue")))
                .matches(
                        strictProject(
                                ImmutableMap.of("leftValue", expression("leftValue")),
                                values("leftKey", "leftKeyHash", "leftValue")));
    }

    @Test
    public void testAllColumnsNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, variable -> true))
                .doesNotFire();
    }

    @Test
    public void testKeysNotNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, variable -> (variable.getName().equals("leftValue") || variable.getName().equals("match"))))
                .doesNotFire();
    }

    @Test
    public void testValueNotNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, variable -> variable.getName().equals("match")))
                .matches(
                        strictProject(
                                ImmutableMap.of("match", expression("match")),
                                semiJoin("leftKey", "rightKey", "match",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "leftKey", expression("leftKey"),
                                                        "leftKeyHash", expression("leftKeyHash")),
                                                values("leftKey", "leftKeyHash", "leftValue")),
                                        values("rightKey"))));
    }

    private static PlanNode buildProjectedSemiJoin(PlanBuilder p, Predicate<VariableReferenceExpression> projectionFilter)
    {
        VariableReferenceExpression match = p.variable("match");
        VariableReferenceExpression leftKey = p.variable("leftKey");
        VariableReferenceExpression leftKeyHash = p.variable("leftKeyHash");
        VariableReferenceExpression leftValue = p.variable("leftValue");
        VariableReferenceExpression rightKey = p.variable("rightKey");
        List<VariableReferenceExpression> outputs = ImmutableList.of(match, leftKey, leftKeyHash, leftValue);
        return p.project(
                identityAssignmentsAsSymbolReferences(
                        outputs.stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.semiJoin(
                        leftKey,
                        rightKey,
                        match,
                        Optional.of(leftKeyHash),
                        Optional.empty(),
                        p.values(leftKey, leftKeyHash, leftValue),
                        p.values(rightKey)));
    }
}
