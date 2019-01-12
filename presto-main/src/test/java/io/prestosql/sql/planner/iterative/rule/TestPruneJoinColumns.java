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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneJoinColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneJoinColumns())
                .on(p -> buildProjectedJoin(p, symbol -> symbol.getName().equals("rightValue")))
                .matches(
                        strictProject(
                                ImmutableMap.of("rightValue", PlanMatchPattern.expression("rightValue")),
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                        Optional.empty(),
                                        values(ImmutableList.of("leftKey", "leftValue")),
                                        values(ImmutableList.of("rightKey", "rightValue")))
                                        .withExactOutputs("rightValue")));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneJoinColumns())
                .on(p -> buildProjectedJoin(p, Predicates.alwaysTrue()))
                .doesNotFire();
    }

    @Test
    public void testCrossJoinDoesNotFire()
    {
        tester().assertThat(new PruneJoinColumns())
                .on(p -> {
                    Symbol leftValue = p.symbol("leftValue");
                    Symbol rightValue = p.symbol("rightValue");
                    return p.project(
                            Assignments.of(),
                            p.join(
                                    JoinNode.Type.INNER,
                                    p.values(leftValue),
                                    p.values(rightValue),
                                    ImmutableList.of(),
                                    ImmutableList.of(leftValue, rightValue),
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .doesNotFire();
    }

    private static PlanNode buildProjectedJoin(PlanBuilder p, Predicate<Symbol> projectionFilter)
    {
        Symbol leftKey = p.symbol("leftKey");
        Symbol leftValue = p.symbol("leftValue");
        Symbol rightKey = p.symbol("rightKey");
        Symbol rightValue = p.symbol("rightValue");
        List<Symbol> outputs = ImmutableList.of(leftKey, leftValue, rightKey, rightValue);
        return p.project(
                Assignments.identity(
                        outputs.stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.join(
                        JoinNode.Type.INNER,
                        p.values(leftKey, leftValue),
                        p.values(rightKey, rightValue),
                        ImmutableList.of(new JoinNode.EquiJoinClause(leftKey, rightKey)),
                        outputs,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }
}
