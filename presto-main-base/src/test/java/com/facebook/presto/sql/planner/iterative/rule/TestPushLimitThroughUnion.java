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
import com.google.common.collect.ImmutableListMultimap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushLimitThroughUnion
        extends BaseRuleTest
{
    @Test
    public void testPushLimitThroughUnion()
    {
        tester().assertThat(new PushLimitThroughUnion())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    return p.limit(1,
                            p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(10, a),
                                            p.values(10, b))));
                })
                .matches(
                        limit(1,
                                union(
                                        limit(1, true, values("a")),
                                        limit(1, true, values("b")))));
    }

    @Test
    public void doesNotFire()
    {
        tester().assertThat(new PushLimitThroughUnion())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    return p.limit(1,
                            p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.limit(1, p.values(5, a)),
                                            p.limit(1, p.values(5, b)))));
                })
                .doesNotFire();
        tester().assertThat(new PushLimitThroughUnion())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    return p.limit(2,
                            p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.limit(1, p.values(5, a)),
                                            p.limit(1, p.values(5, b)))));
                })
                .doesNotFire();
    }
}
