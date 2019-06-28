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
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.castToRowExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestPruneJoinChildrenColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsRereferenced()
    {
        tester().assertThat(new PruneJoinChildrenColumns())
                .on(p -> buildJoin(p, variable -> variable.getName().equals("leftValue")))
                .matches(
                        join(
                                JoinNode.Type.INNER,
                                ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                Optional.of("leftValue > 5"),
                                values("leftKey", "leftKeyHash", "leftValue"),
                                strictProject(
                                        ImmutableMap.of(
                                                "rightKey", PlanMatchPattern.expression("rightKey"),
                                                "rightKeyHash", PlanMatchPattern.expression("rightKeyHash")),
                                        values("rightKey", "rightKeyHash", "rightValue"))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneJoinChildrenColumns())
                .on(p -> buildJoin(p, Predicates.alwaysTrue()))
                .doesNotFire();
    }

    @Test
    public void testCrossJoinDoesNotFire()
    {
        tester().assertThat(new PruneJoinColumns())
                .on(p -> {
                    VariableReferenceExpression leftValue = p.variable("leftValue");
                    VariableReferenceExpression rightValue = p.variable("rightValue");
                    return p.join(
                            JoinNode.Type.INNER,
                            p.values(leftValue),
                            p.values(rightValue),
                            ImmutableList.of(),
                            ImmutableList.of(leftValue, rightValue),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                })
                .doesNotFire();
    }

    private static PlanNode buildJoin(PlanBuilder p, Predicate<VariableReferenceExpression> joinOutputFilter)
    {
        VariableReferenceExpression leftKey = p.variable("leftKey");
        VariableReferenceExpression leftKeyHash = p.variable("leftKeyHash");
        VariableReferenceExpression leftValue = p.variable("leftValue");
        VariableReferenceExpression rightKey = p.variable("rightKey");
        VariableReferenceExpression rightKeyHash = p.variable("rightKeyHash");
        VariableReferenceExpression rightValue = p.variable("rightValue");
        List<VariableReferenceExpression> outputs = ImmutableList.of(leftValue, rightValue);
        return p.join(
                JoinNode.Type.INNER,
                p.values(leftKey, leftKeyHash, leftValue),
                p.values(rightKey, rightKeyHash, rightValue),
                ImmutableList.of(new JoinNode.EquiJoinClause(leftKey, rightKey)),
                outputs.stream()
                        .filter(joinOutputFilter)
                        .collect(toImmutableList()),
                Optional.of(castToRowExpression("leftValue > 5")),
                Optional.of(leftKeyHash),
                Optional.of(rightKeyHash));
    }
}
