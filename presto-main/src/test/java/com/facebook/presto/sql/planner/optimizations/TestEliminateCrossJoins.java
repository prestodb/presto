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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.optimizations.joins.JoinGraph;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.EliminateCrossJoins.isOriginalOrder;
import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

@Test(singleThreaded = true)
public class TestEliminateCrossJoins
{
    PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testIsOriginalOrder()
    {
        assertTrue(isOriginalOrder(ImmutableList.of(0, 1, 2, 3, 4)));
        assertFalse(isOriginalOrder(ImmutableList.of(0, 2, 1, 3, 4)));
    }

    @Test
    public void testJoinOrder()
    {
        PlanNode plan =
                join(
                        join(
                                values(symbol("a")),
                                values(symbol("b"))),
                        values(symbol("c")),
                        symbol("a"), symbol("c"),
                        symbol("c"), symbol("b"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                EliminateCrossJoins.getJoinOrder(joinGraph),
                ImmutableList.of(0, 2, 1));
    }

    @Test
    public void testJoinOrderWithRealCrossJoin()
    {
        PlanNode leftPlan =
                join(
                        join(
                                values(symbol("a")),
                                values(symbol("b"))),
                        values(symbol("c")),
                        symbol("a"), symbol("c"),
                        symbol("c"), symbol("b"));

        PlanNode rightPlan =
                join(
                        join(
                                values(symbol("x")),
                                values(symbol("y"))),
                        values(symbol("z")),
                        symbol("x"), symbol("z"),
                        symbol("z"), symbol("y"));

        PlanNode plan = join(leftPlan, rightPlan);

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                EliminateCrossJoins.getJoinOrder(joinGraph),
                ImmutableList.of(0, 2, 1, 3, 5, 4));
    }

    @Test
    public void testJoinOrderWithMultipleEdgesBetweenNodes()
    {
        PlanNode plan =
                join(
                        join(
                                values(symbol("a")),
                                values(symbol("b1"), symbol("b2"))),
                        values(symbol("c1"), symbol("c2")),
                        symbol("a"), symbol("c1"),
                        symbol("c1"), symbol("b1"),
                        symbol("c2"), symbol("b2"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                EliminateCrossJoins.getJoinOrder(joinGraph),
                ImmutableList.of(0, 2, 1));
    }

    @Test
    public void testDonNotChangeOrderWithoutCrossJoin()
    {
        PlanNode plan =
                join(
                        join(
                                values(symbol("a")),
                                values(symbol("b")),
                                symbol("a"), symbol("b")),
                        values(symbol("c")),
                        symbol("c"), symbol("b"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                EliminateCrossJoins.getJoinOrder(joinGraph),
                ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testDoNotReorderCrossJoins()
    {
        PlanNode plan =
                join(
                        join(
                                values(symbol("a")),
                                values(symbol("b"))),
                        values(symbol("c")),
                        symbol("c"), symbol("b"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                EliminateCrossJoins.getJoinOrder(joinGraph),
                ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testGiveUpOnNonIdentityProjections()
    {
        PlanNode plan =
                join(
                        project(
                                join(
                                        values(symbol("a1")),
                                        values(symbol("b"))),
                                symbol("a2"),
                                new ArithmeticUnaryExpression(MINUS, new SymbolReference("a1"))),
                        values(symbol("c")),
                        symbol("a2"), symbol("c"),
                        symbol("c"), symbol("b"));

        assertEquals(JoinGraph.buildFrom(plan).size(), 2);
    }

    private PlanNode project(PlanNode source, String symbol, Expression expression)
    {
        return new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.of(new Symbol(symbol), expression));
    }

    private String symbol(String name)
    {
        return name;
    }

    private JoinNode join(PlanNode left, PlanNode right, String... symbols)
    {
        checkArgument(symbols.length % 2 == 0);
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

        for (int i = 0; i < symbols.length; i += 2) {
            criteria.add(new JoinNode.EquiJoinClause(new Symbol(symbols[i]), new Symbol(symbols[i + 1])));
        }

        return new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                left,
                right,
                criteria.build(),
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private ValuesNode values(String... symbols)
    {
        return new ValuesNode(
                idAllocator.getNextId(),
                Arrays.stream(symbols).map(Symbol::new).collect(toImmutableList()),
                ImmutableList.of());
    }
}
