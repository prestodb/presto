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
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestRemoveUnreferencedScalarApplyNodes
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveUnreferencedScalarApplyNodes())
                .on(p -> p.apply(
                        assignment(p.variable("z"), p.expression("x IN (y)")),
                        ImmutableList.of(),
                        p.values(p.variable("x")),
                        p.values(p.variable("y"))))
                .doesNotFire();
    }

    @Test
    public void testEmptyAssignments()
    {
        tester().assertThat(new RemoveUnreferencedScalarApplyNodes())
                .on(p -> p.apply(
                        Assignments.of(),
                        ImmutableList.of(),
                        p.values(p.variable("x")),
                        p.values(p.variable("y"))))
                .matches(values("x"));
    }
}
