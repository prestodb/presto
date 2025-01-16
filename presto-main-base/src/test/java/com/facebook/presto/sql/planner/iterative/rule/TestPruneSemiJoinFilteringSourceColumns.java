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
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestPruneSemiJoinFilteringSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllColumnsReferenced()
    {
        tester().assertThat(new PruneSemiJoinFilteringSourceColumns())
                .on(p -> buildSemiJoin(p, variable -> true))
                .matches(
                        semiJoin("leftKey", "rightKey", "match",
                                values("leftKey"),
                                strictProject(
                                        ImmutableMap.of(
                                                "rightKey", expression("rightKey"),
                                                "rightKeyHash", expression("rightKeyHash")),
                                        values("rightKey", "rightKeyHash", "rightValue"))));
    }

    @Test
    public void testAllColumnsNeeded()
    {
        tester().assertThat(new PruneSemiJoinFilteringSourceColumns())
                .on(p -> buildSemiJoin(p, variable -> !variable.getName().equals("rightValue")))
                .doesNotFire();
    }

    private static PlanNode buildSemiJoin(PlanBuilder p, Predicate<VariableReferenceExpression> filteringSourceVariableFilter)
    {
        VariableReferenceExpression match = p.variable("match");
        VariableReferenceExpression leftKey = p.variable("leftKey");
        VariableReferenceExpression rightKey = p.variable("rightKey");
        VariableReferenceExpression rightKeyHash = p.variable("rightKeyHash");
        VariableReferenceExpression rightValue = p.variable("rightValue");
        List<VariableReferenceExpression> filteringSourceVariables = ImmutableList.of(rightKey, rightKeyHash, rightValue);
        List<VariableReferenceExpression> filteredSourceVariables = filteringSourceVariables.stream().filter(filteringSourceVariableFilter).collect(toImmutableList());

        return p.semiJoin(
                leftKey,
                rightKey,
                match,
                Optional.empty(),
                Optional.of(rightKeyHash),
                p.values(leftKey),
                p.values(
                        filteredSourceVariables,
                        ImmutableList.of()));
    }
}
