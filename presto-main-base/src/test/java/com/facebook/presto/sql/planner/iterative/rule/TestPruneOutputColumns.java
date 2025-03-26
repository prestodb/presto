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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictOutput;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneOutputColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneOutputColumns())
                .on(p ->
                {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    return p.output(
                            ImmutableList.of("B label"),
                            ImmutableList.of(b),
                            p.values(a, b));
                })
                .matches(
                        strictOutput(
                                ImmutableList.of("b"),
                                strictProject(
                                        ImmutableMap.of("b", expression("b")),
                                        values("a", "b"))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneOutputColumns())
                .on(p ->
                {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    return p.output(
                            ImmutableList.of("A label", "B label"),
                            ImmutableList.of(a, b),
                            p.values(a, b));
                })
                .doesNotFire();
    }
}
