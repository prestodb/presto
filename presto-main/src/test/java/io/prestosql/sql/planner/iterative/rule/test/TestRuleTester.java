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
package io.prestosql.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestRuleTester
{
    @Test(expectedExceptions = AssertionError.class, expectedExceptionsMessageRegExp = "Plan does not match, expected .* but found .*")
    public void testReportWrongMatch()
    {
        try (RuleTester tester = new RuleTester()) {
            tester.assertThat(new DummyReplaceNodeRule())
                    .on(p ->
                            p.project(
                                    Assignments.of(p.symbol("y"), expression("x")),
                                    p.values(
                                            ImmutableList.of(p.symbol("x")),
                                            ImmutableList.of(ImmutableList.of(expression("1"))))))
                    .matches(
                            values(ImmutableList.of("different"), ImmutableList.of()));
        }
    }

    private static class DummyReplaceNodeRule
            implements Rule<PlanNode>
    {
        @Override
        public Pattern<PlanNode> getPattern()
        {
            return Pattern.typeOf(PlanNode.class);
        }

        @Override
        public Result apply(PlanNode node, Captures captures, Context context)
        {
            return Result.ofPlanNode(node.replaceChildren(node.getSources()));
        }
    }
}
