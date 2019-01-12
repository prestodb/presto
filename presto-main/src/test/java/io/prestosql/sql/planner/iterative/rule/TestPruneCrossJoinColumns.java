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
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneCrossJoinColumns
        extends BaseRuleTest
{
    @Test
    public void testLeftInputNotReferenced()
    {
        tester().assertThat(new PruneCrossJoinColumns())
                .on(p -> buildProjectedCrossJoin(p, symbol -> symbol.getName().equals("rightValue")))
                .matches(
                        strictProject(
                                ImmutableMap.of("rightValue", PlanMatchPattern.expression("rightValue")),
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        strictProject(
                                                ImmutableMap.of(),
                                                values(ImmutableList.of("leftValue"))),
                                        values(ImmutableList.of("rightValue")))
                                        .withExactOutputs("rightValue")));
    }

    @Test
    public void testRightInputNotReferenced()
    {
        tester().assertThat(new PruneCrossJoinColumns())
                .on(p -> buildProjectedCrossJoin(p, symbol -> symbol.getName().equals("leftValue")))
                .matches(
                        strictProject(
                                ImmutableMap.of("leftValue", PlanMatchPattern.expression("leftValue")),
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        values(ImmutableList.of("leftValue")),
                                        strictProject(
                                                ImmutableMap.of(),
                                                values(ImmutableList.of("rightValue"))))
                                        .withExactOutputs("leftValue")));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneCrossJoinColumns())
                .on(p -> buildProjectedCrossJoin(p, Predicates.alwaysTrue()))
                .doesNotFire();
    }

    private static PlanNode buildProjectedCrossJoin(PlanBuilder p, Predicate<Symbol> projectionFilter)
    {
        Symbol leftValue = p.symbol("leftValue");
        Symbol rightValue = p.symbol("rightValue");
        List<Symbol> outputs = ImmutableList.of(leftValue, rightValue);
        return p.project(
                Assignments.identity(
                        outputs.stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.join(
                        JoinNode.Type.INNER,
                        p.values(leftValue),
                        p.values(rightValue),
                        ImmutableList.of(),
                        outputs,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }
}
