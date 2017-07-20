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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.iterative.rule.MultiJoinNode.toMultiJoinNode;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Type.ADD;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.NOT_EQUAL;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestMultiJoinNodeBuilder
{
    private final LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder().build());
    private static final int DEFAULT_JOIN_LIMIT = 10;

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDoesNotFireForOuterJoins()
    {
        PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), queryRunner.getMetadata());
        JoinNode outerJoin = p.join(
                JoinNode.Type.FULL,
                p.values(p.symbol("A1", BIGINT)),
                p.values(p.symbol("B1", BIGINT)),
                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                Optional.empty());
        toMultiJoinNode(outerJoin, queryRunner.getLookup(), DEFAULT_JOIN_LIMIT);
    }

    @Test
    public void testDoesNotConvertNestedOuterJoins()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        Symbol a1 = planBuilder.symbol("A1", BIGINT);
        Symbol b1 = planBuilder.symbol("B1", BIGINT);
        Symbol c1 = planBuilder.symbol("C1", BIGINT);
        JoinNode leftJoin = planBuilder.join(
                LEFT,
                planBuilder.values(a1),
                planBuilder.values(b1),
                ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1),
                Optional.empty());
        ValuesNode valuesC = planBuilder.values(c1);
        JoinNode joinNode = planBuilder.join(
                INNER,
                leftJoin,
                valuesC,
                ImmutableList.of(new JoinNode.EquiJoinClause(a1, c1)),
                ImmutableList.of(a1, b1, c1),
                Optional.empty());

        MultiJoinNode expected = new MultiJoinNode(ImmutableList.of(leftJoin, valuesC), new ComparisonExpression(EQUAL, a1.toSymbolReference(), c1.toSymbolReference()), ImmutableList.of(a1, b1, c1));
        assertMultijoinEquals(toMultiJoinNode(joinNode, queryRunner.getLookup(), DEFAULT_JOIN_LIMIT), expected);
    }

    @Test
    public void testRetainsOutputSymbols()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        Symbol a1 = planBuilder.symbol("A1", BIGINT);
        Symbol b1 = planBuilder.symbol("B1", BIGINT);
        Symbol b2 = planBuilder.symbol("B2", BIGINT);
        Symbol c1 = planBuilder.symbol("C1", BIGINT);
        Symbol c2 = planBuilder.symbol("C2", BIGINT);
        ValuesNode valuesA = planBuilder.values(a1);
        ValuesNode valuesB = planBuilder.values(b1, b2);
        ValuesNode valuesC = planBuilder.values(c1, c2);
        JoinNode joinNode = planBuilder.join(
                INNER,
                valuesA,
                planBuilder.join(
                        INNER,
                        valuesB,
                        valuesC,
                        ImmutableList.of(new JoinNode.EquiJoinClause(b1, c1)),
                        ImmutableList.of(
                                b1,
                                b2,
                                c1,
                                c2),
                        Optional.empty()),
                ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1),
                Optional.empty());
        MultiJoinNode expected = new MultiJoinNode(
                ImmutableList.of(valuesA, valuesB, valuesC),
                and(new ComparisonExpression(EQUAL, b1.toSymbolReference(), c1.toSymbolReference()), new ComparisonExpression(EQUAL, a1.toSymbolReference(), b1.toSymbolReference())),
                ImmutableList.of(a1, b1));
        assertMultijoinEquals(toMultiJoinNode(joinNode, queryRunner.getLookup(), DEFAULT_JOIN_LIMIT), expected);
    }

    @Test
    public void testCombinesCriteriaAndFilters()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        Symbol a1 = planBuilder.symbol("A1", BIGINT);
        Symbol b1 = planBuilder.symbol("B1", BIGINT);
        Symbol b2 = planBuilder.symbol("B2", BIGINT);
        Symbol c1 = planBuilder.symbol("C1", BIGINT);
        Symbol c2 = planBuilder.symbol("C2", BIGINT);
        ValuesNode valuesA = planBuilder.values(a1);
        ValuesNode valuesB = planBuilder.values(b1, b2);
        ValuesNode valuesC = planBuilder.values(c1, c2);
        Expression bcFilter = and(
                new ComparisonExpression(GREATER_THAN, c2.toSymbolReference(), new LongLiteral("0")),
                new ComparisonExpression(NOT_EQUAL, c2.toSymbolReference(), new LongLiteral("7")),
                new ComparisonExpression(GREATER_THAN, b2.toSymbolReference(), c2.toSymbolReference()));
        ComparisonExpression abcFilter = new ComparisonExpression(
                LESS_THAN,
                new ArithmeticBinaryExpression(ADD, a1.toSymbolReference(), c1.toSymbolReference()),
                b1.toSymbolReference());
        JoinNode joinNode = planBuilder.join(
                INNER,
                valuesA,
                planBuilder.join(
                        INNER,
                        valuesB,
                        valuesC,
                        ImmutableList.of(new JoinNode.EquiJoinClause(b1, c1)),
                        ImmutableList.of(
                                b1,
                                b2,
                                c1,
                                c2),
                        Optional.of(bcFilter)),
                ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1, b2, c1, c2),
                Optional.of(abcFilter));
        MultiJoinNode expected = new MultiJoinNode(
                ImmutableList.of(valuesA, valuesB, valuesC),
                and(new ComparisonExpression(EQUAL, b1.toSymbolReference(), c1.toSymbolReference()), new ComparisonExpression(EQUAL, a1.toSymbolReference(), b1.toSymbolReference()), bcFilter, abcFilter),
                ImmutableList.of(a1, b1, b2, c1, c2));
        assertMultijoinEquals(toMultiJoinNode(joinNode, queryRunner.getLookup(), DEFAULT_JOIN_LIMIT), expected);
    }

    @Test
    public void testConvertsBushyTrees()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        Symbol a1 = planBuilder.symbol("A1", BIGINT);
        Symbol b1 = planBuilder.symbol("B1", BIGINT);
        Symbol c1 = planBuilder.symbol("C1", BIGINT);
        Symbol d1 = planBuilder.symbol("D1", BIGINT);
        Symbol d2 = planBuilder.symbol("D2", BIGINT);
        Symbol e1 = planBuilder.symbol("E1", BIGINT);
        Symbol e2 = planBuilder.symbol("E2", BIGINT);
        ValuesNode valuesA = planBuilder.values(a1);
        ValuesNode valuesB = planBuilder.values(b1);
        ValuesNode valuesC = planBuilder.values(c1);
        ValuesNode valuesD = planBuilder.values(d1, d2);
        ValuesNode valuesE = planBuilder.values(e1, e2);
        JoinNode joinNode = planBuilder.join(
                INNER,
                planBuilder.join(
                        INNER,
                        planBuilder.join(
                                INNER,
                                valuesA,
                                valuesB,
                                ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                                ImmutableList.of(a1, b1),
                                Optional.empty()),
                        valuesC,
                        ImmutableList.of(new JoinNode.EquiJoinClause(a1, c1)),
                        ImmutableList.of(a1, b1, c1),
                        Optional.empty()),
                planBuilder.join(
                        INNER,
                        valuesD,
                        valuesE,
                        ImmutableList.of(
                                new JoinNode.EquiJoinClause(d1, e1),
                                new JoinNode.EquiJoinClause(d2, e2)),
                        ImmutableList.of(
                                d1,
                                d2,
                                e1,
                                e2),
                        Optional.empty()),
                ImmutableList.of(new JoinNode.EquiJoinClause(b1, e1)),
                ImmutableList.of(
                        a1,
                        b1,
                        c1,
                        d1,
                        d2,
                        e1,
                        e2),
                Optional.empty());
        MultiJoinNode expected = new MultiJoinNode(
                ImmutableList.of(valuesA, valuesB, valuesC, valuesD, valuesE),
                and(
                        new ComparisonExpression(EQUAL, a1.toSymbolReference(), b1.toSymbolReference()),
                        new ComparisonExpression(EQUAL, a1.toSymbolReference(), c1.toSymbolReference()),
                        new ComparisonExpression(EQUAL, d1.toSymbolReference(), e1.toSymbolReference()),
                        new ComparisonExpression(EQUAL, d2.toSymbolReference(), e2.toSymbolReference()),
                        new ComparisonExpression(EQUAL, b1.toSymbolReference(), e1.toSymbolReference())),
                ImmutableList.of(a1, b1, c1, d1, d2, e1, e2));
        assertMultijoinEquals(toMultiJoinNode(joinNode, queryRunner.getLookup(), 5), expected);
    }

    @Test
    public void testMoreThanJoinLimit()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        Symbol a1 = planBuilder.symbol("A1", BIGINT);
        Symbol b1 = planBuilder.symbol("B1", BIGINT);
        Symbol c1 = planBuilder.symbol("C1", BIGINT);
        Symbol d1 = planBuilder.symbol("D1", BIGINT);
        Symbol d2 = planBuilder.symbol("D2", BIGINT);
        Symbol e1 = planBuilder.symbol("E1", BIGINT);
        Symbol e2 = planBuilder.symbol("E2", BIGINT);
        ValuesNode valuesA = planBuilder.values(a1);
        ValuesNode valuesB = planBuilder.values(b1);
        ValuesNode valuesC = planBuilder.values(c1);
        ValuesNode valuesD = planBuilder.values(d1, d2);
        ValuesNode valuesE = planBuilder.values(e1, e2);
        JoinNode join1 = planBuilder.join(
                INNER,
                valuesA,
                valuesB,
                ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1),
                Optional.empty());
        JoinNode join2 = planBuilder.join(
                INNER,
                valuesD,
                valuesE,
                ImmutableList.of(
                        new JoinNode.EquiJoinClause(d1, e1),
                        new JoinNode.EquiJoinClause(d2, e2)),
                ImmutableList.of(
                        d1,
                        d2,
                        e1,
                        e2),
                Optional.empty());
        JoinNode joinNode = planBuilder.join(
                INNER,
                planBuilder.join(
                        INNER,
                        join1,
                        valuesC,
                        ImmutableList.of(new JoinNode.EquiJoinClause(a1, c1)),
                        ImmutableList.of(a1, b1, c1),
                        Optional.empty()),
                join2,
                ImmutableList.of(new JoinNode.EquiJoinClause(b1, e1)),
                ImmutableList.of(
                        a1,
                        b1,
                        c1,
                        d1,
                        d2,
                        e1,
                        e2),
                Optional.empty());
        MultiJoinNode expected = new MultiJoinNode(
                ImmutableList.of(join1, join2, valuesC),
                and(
                        new ComparisonExpression(EQUAL, a1.toSymbolReference(), c1.toSymbolReference()),
                        new ComparisonExpression(EQUAL, b1.toSymbolReference(), e1.toSymbolReference())),
                ImmutableList.of(a1, b1, c1, d1, d2, e1, e2));
        assertMultijoinEquals(toMultiJoinNode(joinNode, queryRunner.getLookup(), 3), expected);
    }

    private static void assertMultijoinEquals(MultiJoinNode actual, MultiJoinNode expected)
    {
        assertEquals(ImmutableSet.copyOf(actual.getSources()), ImmutableSet.copyOf(expected.getSources()));
        assertEquals(ImmutableSet.copyOf(extractConjuncts(actual.getFilter())), ImmutableSet.copyOf(extractConjuncts(expected.getFilter())));
        assertEquals(actual.getOutputSymbols(), expected.getOutputSymbols());
    }
}
