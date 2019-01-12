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
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestRemoveUnreferencedScalarApplyNodes
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveUnreferencedScalarApplyNodes())
                .on(p -> p.apply(
                        Assignments.of(p.symbol("z"), p.expression("x IN (y)")),
                        ImmutableList.of(),
                        p.values(p.symbol("x")),
                        p.values(p.symbol("y"))))
                .doesNotFire();
    }

    @Test
    public void testEmptyAssignments()
    {
        tester().assertThat(new RemoveUnreferencedScalarApplyNodes())
                .on(p -> p.apply(
                        Assignments.of(),
                        ImmutableList.of(),
                        p.values(p.symbol("x")),
                        p.values(p.symbol("y"))))
                .matches(values("x"));
    }
}
