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

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;

public class TestCanonicalizeProjectExpressions
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireForExpressionsInCanonicalForm()
    {
        tester().assertThat(new CanonicalizeProjectExpressions())
                .on(p -> p.project(Assignments.of(p.symbol("x"), FALSE_LITERAL), p.values()))
                .doesNotFire();
    }

    @Test
    public void testCanonicalizesExpressions()
    {
        tester().assertThat(new CanonicalizeProjectExpressions())
                .on(p -> p.project(
                        Assignments.of(p.symbol("y"), p.expression("x IS NOT NULL")),
                        p.values(p.symbol("x"))))
                .matches(project(ImmutableMap.of("y", expression("NOT (x IS NULL)")), values("x")));
    }
}
