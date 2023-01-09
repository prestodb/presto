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

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.optimizations.joins.JoinGraph;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.EliminateCrossJoins.getJoinOrder;
import static com.facebook.presto.sql.planner.iterative.rule.EliminateCrossJoins.isOriginalOrder;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestEliminateCrossJoins
        extends BaseRuleTest
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testEliminateCrossJoin()
    {
        tester().assertThat(new EliminateCrossJoins())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .on(crossJoinAndJoin(INNER))
                .matches(
                        join(INNER,
                                ImmutableList.of(aliases -> new EquiJoinClause(variable("cyVariable"), variable("byVariable"))),
                                join(INNER,
                                        ImmutableList.of(aliases -> new EquiJoinClause(variable("axVariable"), variable("cxVariable"))),
                                        any(),
                                        any()),
                                any()));
    }

    @Test
    public void testRetainOutgoingGroupReferences()
    {
        tester().assertThat(new EliminateCrossJoins())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .on(crossJoinAndJoin(INNER))
                .matches(
                        node(JoinNode.class,
                                node(JoinNode.class,
                                        node(GroupReference.class),
                                        node(GroupReference.class)),
                                node(GroupReference.class)));
    }

    @Test
    public void testDoNotReorderOuterJoin()
    {
        tester().assertThat(new EliminateCrossJoins())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .on(crossJoinAndJoin(JoinNode.Type.LEFT))
                .doesNotFire();
    }

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
                joinNode(
                        joinNode(
                                values(variable("a")),
                                values(variable("b"))),
                        values(variable("c")),
                        variable("a"), variable("c"),
                        variable("c"), variable("b"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                getJoinOrder(joinGraph),
                ImmutableList.of(0, 2, 1));
    }

    @Test
    public void testJoinOrderWithRealCrossJoin()
    {
        PlanNode leftPlan =
                joinNode(
                        joinNode(
                                values(variable("a")),
                                values(variable("b"))),
                        values(variable("c")),
                        variable("a"), variable("c"),
                        variable("c"), variable("b"));

        PlanNode rightPlan =
                joinNode(
                        joinNode(
                                values(variable("x")),
                                values(variable("y"))),
                        values(variable("z")),
                        variable("x"), variable("z"),
                        variable("z"), variable("y"));

        PlanNode plan = joinNode(leftPlan, rightPlan);

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                getJoinOrder(joinGraph),
                ImmutableList.of(0, 2, 1, 3, 5, 4));
    }

    @Test
    public void testJoinOrderWithMultipleEdgesBetweenNodes()
    {
        PlanNode plan =
                joinNode(
                        joinNode(
                                values(variable("a")),
                                values(variable("b1"), variable("b2"))),
                        values(variable("c1"), variable("c2")),
                        variable("a"), variable("c1"),
                        variable("c1"), variable("b1"),
                        variable("c2"), variable("b2"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                getJoinOrder(joinGraph),
                ImmutableList.of(0, 2, 1));
    }

    @Test
    public void testDonNotChangeOrderWithoutCrossJoin()
    {
        PlanNode plan =
                joinNode(
                        joinNode(
                                values(variable("a")),
                                values(variable("b")),
                                variable("a"), variable("b")),
                        values(variable("c")),
                        variable("c"), variable("b"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                getJoinOrder(joinGraph),
                ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testDoNotReorderCrossJoins()
    {
        PlanNode plan =
                joinNode(
                        joinNode(
                                values(variable("a")),
                                values(variable("b"))),
                        values(variable("c")),
                        variable("c"), variable("b"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                getJoinOrder(joinGraph),
                ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testGiveUpOnNonIdentityProjections()
    {
        PlanNode plan =
                joinNode(
                        projectNode(
                                joinNode(
                                        values(variable("a1")),
                                        values(variable("b"))),
                                variable("a2"),
                                new ArithmeticUnaryExpression(MINUS, new SymbolReference("a1"))),
                        values(variable("c")),
                        variable("a2"), variable("c"),
                        variable("c"), variable("b"));

        assertEquals(JoinGraph.buildFrom(plan).size(), 2);
    }

    private Function<PlanBuilder, PlanNode> crossJoinAndJoin(JoinNode.Type secondJoinType)
    {
        return p -> {
            VariableReferenceExpression axVariable = p.variable("axVariable");
            VariableReferenceExpression byVariable = p.variable("byVariable");
            VariableReferenceExpression cxVariable = p.variable("cxVariable");
            VariableReferenceExpression cyVariable = p.variable("cyVariable");

            // (a inner join b) inner join c on c.x = a.x and c.y = b.y
            return p.join(INNER,
                    p.join(secondJoinType,
                            p.values(axVariable),
                            p.values(byVariable)),
                    p.values(cxVariable, cyVariable),
                    new EquiJoinClause(p.variable(cxVariable), p.variable(axVariable)),
                    new EquiJoinClause(p.variable(cyVariable), p.variable(byVariable)));
        };
    }

    private PlanNode projectNode(PlanNode source, VariableReferenceExpression variable, Expression expression)
    {
        return new ProjectNode(
                idAllocator.getNextId(),
                source,
                assignment(variable, expression));
    }

    private VariableReferenceExpression variable(String name)
    {
        return new VariableReferenceExpression(Optional.empty(), name, BIGINT);
    }

    private JoinNode joinNode(PlanNode left, PlanNode right, VariableReferenceExpression... variables)
    {
        checkArgument(variables.length % 2 == 0);
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

        for (int i = 0; i < variables.length; i += 2) {
            criteria.add(new JoinNode.EquiJoinClause(variables[i], variables[i + 1]));
        }

        return new JoinNode(
                Optional.empty(),
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                left,
                right,
                criteria.build(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    private ValuesNode values(VariableReferenceExpression... variables)
    {
        return new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                Arrays.asList(variables),
                ImmutableList.of(),
                Optional.empty());
    }
}
