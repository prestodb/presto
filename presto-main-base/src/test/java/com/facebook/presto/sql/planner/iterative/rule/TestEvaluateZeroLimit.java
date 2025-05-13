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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;

public class TestEvaluateZeroLimit
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new EvaluateZeroLimit())
                .on(p ->
                        p.limit(
                                1,
                                p.values(p.variable("a"))))
                .doesNotFire();
    }

    @Test
    public void test()
    {
        tester().assertThat(new EvaluateZeroLimit())
                .on(p ->
                        p.limit(
                                0,
                                p.registerVariable(p.variable("b"))
                                        .filter(
                                                p.rowExpression("b > 5"),
                                                p.values(
                                                        ImmutableList.of(p.variable("a"), p.variable("b")),
                                                        ImmutableList.of(
                                                                constantExpressions(BIGINT, 1L, 10L),
                                                                constantExpressions(BIGINT, 2L, 11L))))))
                // TODO: verify contents
                .matches(values(ImmutableMap.of()));
    }
}
