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
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.SampleNode.Type;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expressions;

public class TestEvaluateZeroSample
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new EvaluateZeroSample())
                .on(p ->
                        p.sample(
                                0.15,
                                Type.BERNOULLI,
                                p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void test()
    {
        tester().assertThat(new EvaluateZeroSample())
                .on(p ->
                        p.sample(
                                0,
                                Type.BERNOULLI,
                                p.filter(
                                        expression("b > 5"),
                                        p.values(
                                                ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                                ImmutableList.of(
                                                        expressions("1", "10"),
                                                        expressions("2", "11"))))))
                // TODO: verify contents
                .matches(values(ImmutableMap.of()));
    }
}
