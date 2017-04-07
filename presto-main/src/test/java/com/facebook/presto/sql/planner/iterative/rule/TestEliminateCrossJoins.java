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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.REORDER_JOINS;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestEliminateCrossJoins
{
    private final RuleTester tester = new RuleTester();

    @Test
    public void testEliminateCrossJoin()
    {
        tester.assertThat(new EliminateCrossJoins())
                .setSystemProperty(REORDER_JOINS, "true")
                .on(crossJoinAndJoin(INNER))
                .matches(
                        project(
                                join(INNER,
                                        ImmutableList.of(aliases -> new EquiJoinClause(new Symbol("cySymbol"), new Symbol("bySymbol"))),
                                        join(INNER,
                                                ImmutableList.of(aliases -> new EquiJoinClause(new Symbol("axSymbol"), new Symbol("cxSymbol"))),
                                                any(),
                                                any()
                                        ),
                                        any()
                                )
                        )
                );
    }

    @Test
    public void testRetainOutgoingGroupReferences()
    {
        tester.assertThat(new EliminateCrossJoins())
                .setSystemProperty(REORDER_JOINS, "true")
                .on(crossJoinAndJoin(INNER))
                .matches(
                        any(
                                node(JoinNode.class,
                                        node(JoinNode.class,
                                                node(GroupReference.class),
                                                node(GroupReference.class)
                                        ),
                                        node(GroupReference.class)
                                )
                        )
                );
    }

    @Test
    public void testDoNotReorderOuterJoin()
    {
        tester.assertThat(new EliminateCrossJoins())
                .setSystemProperty(REORDER_JOINS, "true")
                .on(crossJoinAndJoin(JoinNode.Type.LEFT))
                .doesNotFire();
    }

    private Function<PlanBuilder, PlanNode> crossJoinAndJoin(JoinNode.Type secondJoinType)
    {
        return p -> {
            Symbol axSymbol = p.symbol("axSymbol", BIGINT);
            Symbol bySymbol = p.symbol("bySymbol", BIGINT);
            Symbol cxSymbol = p.symbol("cxSymbol", BIGINT);
            Symbol cySymbol = p.symbol("cySymbol", BIGINT);

            // (a inner join b) inner join c on c.x = a.x and c.y = b.y
            return p.join(INNER,
                    p.join(secondJoinType,
                            p.values(axSymbol),
                            p.values(bySymbol)
                    ),
                    p.values(cxSymbol, cySymbol),
                    new EquiJoinClause(cxSymbol, axSymbol),
                    new EquiJoinClause(cySymbol, bySymbol)
            );
        };
    }
}
