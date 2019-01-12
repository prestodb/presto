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

import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class TestPushLimitThroughProject
        extends BaseRuleTest
{
    @Test
    public void testPushdownLimitNonIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(1,
                            p.project(
                                    Assignments.of(a, TRUE_LITERAL),
                                    p.values()));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("true")),
                                limit(1, values())));
    }

    @Test
    public void testDoesntPushdownLimitThroughIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(1,
                            p.project(
                                    Assignments.of(a, a.toSymbolReference()),
                                    p.values(a)));
                }).doesNotFire();
    }
}
