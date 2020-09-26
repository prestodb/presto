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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.Session;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.PushTopNThroughOuterJoin;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.plan.TopNNode.Step.FINAL;
import static com.facebook.presto.spi.plan.TopNNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.topN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.FIRST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestPushTopNThroughOuterJoin
        extends BaseRuleTest
{
    @Test
    public void testPushTopNThroughLeftJoin()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("leftKey");
                    VariableReferenceExpression rightKey = p.variable("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            PARTIAL,
                            p.join(
                                    LEFT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                topN(1, ImmutableList.of(sort("leftKey", ASCENDING, FIRST)), PARTIAL, values("leftKey")),
                                values("rightKey")));
    }

    @Test
    public void testPushTopNThroughRightJoin()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("leftKey");
                    VariableReferenceExpression rightKey = p.variable("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(rightKey),
                            PARTIAL,
                            p.join(
                                    RIGHT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        join(
                                RIGHT,
                                ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                values("leftKey"),
                                topN(1, ImmutableList.of(sort("rightKey", ASCENDING, FIRST)), PARTIAL, values("rightKey"))));
    }

    @Test
    public void testPushTopNThroughLeftDeepJoin()
    {
        // TopN (order: Y, source: (( Values(5) as X RJ Values(5) as Y on X=Y) LJ Values(5) as Z ON Y=Z) =>
        // (( Values(5) as X RJ (TopN (order: Y, source: Values(5) as Y) on X=Y)) LJ Values(5) as Z ON Y=Z)
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1")
                .setSystemProperty("iterative_optimizer_enabled", "true")
                .setSystemProperty("iterative_optimizer_timeout", "1ms");
        LocalQueryRunner queryRunner = new LocalQueryRunner(sessionBuilder.build());

        IterativeOptimizer optimizer = new IterativeOptimizer(
                new RuleStatsRecorder(),
                queryRunner.getStatsCalculator(),
                queryRunner.getCostCalculator(),
                ImmutableSet.of(
                        new PushTopNThroughOuterJoin()));

        tester().assertThat(optimizer)
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x");
                    VariableReferenceExpression y = p.variable("y");
                    VariableReferenceExpression z = p.variable("z");
                    return p.topN(
                            1,
                            ImmutableList.of(y),
                            PARTIAL,
                            p.join(
                                    LEFT,
                                    p.join(
                                            RIGHT,
                                            p.values(5, x),
                                            p.values(5, y),
                                            new JoinNode.EquiJoinClause(x, y)
                                    ), p.values(5, z),
                                    new JoinNode.EquiJoinClause(y, z)));
                }).matches(
                join(LEFT,
                        ImmutableList.of(equiJoinClause("y", "z")),
                        join(
                                RIGHT,
                                ImmutableList.of(equiJoinClause("x", "y")),
                                values("x"),
                                topN(1,
                                        ImmutableList.of(sort("y", ASCENDING, FIRST)), PARTIAL, values("y"))
                        ), values("z")));
    }

    @Test
    public void testFullJoin()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("leftKey");
                    VariableReferenceExpression rightKey = p.variable("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(rightKey),
                            PARTIAL,
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();

        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("leftKey");
                    VariableReferenceExpression rightKey = p.variable("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            PARTIAL,
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPushTopNWhenSymbolsFromBothSources()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("leftKey");
                    VariableReferenceExpression rightKey = p.variable("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey, rightKey),
                            PARTIAL,
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPushWhenAlreadyLimited()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("leftKey");
                    VariableReferenceExpression rightKey = p.variable("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            PARTIAL,
                            p.join(
                                    LEFT,
                                    p.limit(1, p.values(5, leftKey)),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPushWhenStepNotPartial()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("leftKey");
                    VariableReferenceExpression rightKey = p.variable("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            FINAL,
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }
}
