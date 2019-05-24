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
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;

public class TestPushLimitThroughOuterJoin
        extends BaseRuleTest
{
    @Test
    public void testPushLimitThroughLeftJoin()
    {
        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("leftKey");
                    VariableReferenceExpression rightKey = p.variable("rightKey");
                    return p.limit(1,
                            p.join(
                                    LEFT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        limit(1,
                                join(
                                        LEFT,
                                        ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                        limit(1, true, values("leftKey")),
                                        values("rightKey"))));
    }

    @Test
    public void testDoesNotPushThroughFullOuterJoin()
    {
        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("leftKey");
                    VariableReferenceExpression rightKey = p.variable("rightKey");
                    return p.limit(1,
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPushWhenAlreadyLimited()
    {
        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("leftKey");
                    VariableReferenceExpression rightKey = p.variable("rightKey");
                    return p.limit(1,
                            p.join(
                                    LEFT,
                                    p.limit(1, p.values(5, leftKey)),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }
}
