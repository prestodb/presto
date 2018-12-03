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
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.topN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.FIRST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushTopNThroughOuterJoin
        extends BaseRuleTest
{
    @Test
    public void testPushTopNThroughLeftJoin()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            p.project(Assignments.of(leftKey, new SymbolReference("leftKey"), rightKey, new SymbolReference("rightKey")),
                                    p.join(
                                            LEFT,
                                            p.values(5, leftKey),
                                            p.values(5, rightKey),
                                            new JoinNode.EquiJoinClause(leftKey, rightKey))));
                })
                .matches(
                        topN(
                                1,
                                ImmutableList.of(sort("leftKey", ASCENDING, FIRST)),
                                project(
                                        join(
                                                LEFT,
                                                ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                                topN(1, ImmutableList.of(sort("leftKey", ASCENDING, FIRST)), values("leftKey")),
                                                values("rightKey")))));
    }

    /**
     * Push topN to right source of join
     */
    @Test
    public void testPushRightwardsTopNThroughFullOuterJoin()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(rightKey),
                            p.project(Assignments.of(leftKey, new SymbolReference("leftKey"), rightKey, new SymbolReference("rightKey")),
                                    p.join(
                                            FULL,
                                            p.values(5, leftKey),
                                            p.values(5, rightKey),
                                            new JoinNode.EquiJoinClause(leftKey, rightKey))));
                })
                .matches(
                        topN(
                                1,
                                ImmutableList.of(sort("rightKey", ASCENDING, FIRST)),
                                project(
                                        join(
                                                FULL,
                                                ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                                values("leftKey"),
                                                topN(1, ImmutableList.of(sort("rightKey", ASCENDING, FIRST)), values("rightKey"))))));
    }

    /**
     * Push topN to left source of join
     */
    @Test
    public void testPushLeftwardsTopNThroughFullOuterJoin()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            p.project(Assignments.of(leftKey, new SymbolReference("leftKey"), rightKey, new SymbolReference("rightKey")),
                                    p.join(
                                            FULL,
                                            p.values(5, leftKey),
                                            p.values(5, rightKey),
                                            new JoinNode.EquiJoinClause(leftKey, rightKey))));
                })
                .matches(
                        topN(
                                1,
                                ImmutableList.of(sort("leftKey", ASCENDING, FIRST)),
                                project(
                                        join(
                                                FULL,
                                                ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                                topN(1, ImmutableList.of(sort("leftKey", ASCENDING, FIRST)), values("leftKey")),
                                                values("rightKey")))));
    }

    @Test
    public void testDoNotPushWhenAlreadyLimited()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            p.project(Assignments.of(leftKey, new SymbolReference("leftKey"), rightKey, new SymbolReference("rightKey")),
                                    p.join(
                                            LEFT,
                                            p.limit(1, p.values(5, leftKey)),
                                            p.values(5, rightKey),
                                            new JoinNode.EquiJoinClause(leftKey, rightKey))));
                })
                .doesNotFire();
    }
}
