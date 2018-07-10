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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static java.util.Collections.emptyList;

public class TestRemoveUnreferencedScalarJoinNodes
        extends BaseRuleTest
{
    @Test
    public void testRemoveUnreferencedLeft()
    {
        tester().assertThat(new RemoveUnreferencedScalarJoinNodes())
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("x", BigintType.BIGINT)),
                        p.nodeWithTrait(CardinalityTrait.scalar())))
                .matches(values("x"));
    }

    @Test
    public void testRemoveUnreferencedLeftWithFilterExpression()
    {
        tester().assertThat(new RemoveUnreferencedScalarJoinNodes())
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("x", BigintType.BIGINT)),
                        p.nodeWithTrait(CardinalityTrait.scalar()),
                        PlanBuilder.expression("x = 3")))
                .matches(
                        filter("x = 3",
                                values("x")));
    }

    @Test
    public void testRemoveUnreferencedSubquery()
    {
        tester().assertThat(new RemoveUnreferencedScalarJoinNodes())
                .on(p -> p.join(
                        INNER,
                        p.nodeWithTrait(CardinalityTrait.scalar()),
                        p.values(p.symbol("x", BigintType.BIGINT))))
                .matches(values("x"));
    }

    @Test
    public void testRemoveUnreferencedSubqueryWithFilterExpression()
    {
        tester().assertThat(new RemoveUnreferencedScalarJoinNodes())
                .on(p -> p.join(
                        INNER,
                        p.nodeWithTrait(CardinalityTrait.scalar()),
                        p.values(p.symbol("x", BigintType.BIGINT)),
                        PlanBuilder.expression("x = 3")))
                .matches(
                        filter("x = 3",
                                values("x")));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveUnreferencedScalarJoinNodes())
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("x", BigintType.BIGINT)),
                        p.values(emptyList(), ImmutableList.of(emptyList()))))
                .doesNotFire();

        tester().assertThat(new RemoveUnreferencedScalarJoinNodes())
                .on(p -> p.join(
                        INNER,
                        p.values(emptyList(), ImmutableList.of(emptyList())),
                        p.values(p.symbol("x", BigintType.BIGINT))))
                .doesNotFire();

        tester().assertThat(new RemoveUnreferencedScalarJoinNodes())
                .on(p -> p.join(
                        INNER,
                        p.nodeWithTrait(CardinalityTrait.exactly(2L)),
                        p.values()))
                .doesNotFire();

        tester().assertThat(new RemoveUnreferencedScalarJoinNodes())
                .on(p -> p.join(
                        INNER,
                        p.nodeWithTrait(CardinalityTrait.scalar()),
                        p.values(),
                        new JoinNode.EquiJoinClause(p.symbol("x"), p.symbol("x"))))
                .doesNotFire();
    }
}
