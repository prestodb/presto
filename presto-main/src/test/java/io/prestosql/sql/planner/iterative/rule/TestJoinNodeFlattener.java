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

package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.JoinNode.EquiJoinClause;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.prestosql.sql.ExpressionUtils.and;
import static io.prestosql.sql.planner.iterative.Lookup.noLookup;
import static io.prestosql.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode.toMultiJoinNode;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestJoinNodeFlattener
{
    private static final int DEFAULT_JOIN_LIMIT = 10;

    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = new LocalQueryRunner(testSessionBuilder().build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDoesNotAllowOuterJoin()
    {
        PlanBuilder p = planBuilder();
        Symbol a1 = p.symbol("A1");
        Symbol b1 = p.symbol("B1");
        JoinNode outerJoin = p.join(
                FULL,
                p.values(a1),
                p.values(b1),
                ImmutableList.of(equiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1),
                Optional.empty());
        toMultiJoinNode(outerJoin, noLookup(), DEFAULT_JOIN_LIMIT);
    }

    @Test
    public void testDoesNotConvertNestedOuterJoins()
    {
        PlanBuilder p = planBuilder();
        Symbol a1 = p.symbol("A1");
        Symbol b1 = p.symbol("B1");
        Symbol c1 = p.symbol("C1");
        JoinNode leftJoin = p.join(
                LEFT,
                p.values(a1),
                p.values(b1),
                ImmutableList.of(equiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1),
                Optional.empty());
        ValuesNode valuesC = p.values(c1);
        JoinNode joinNode = p.join(
                INNER,
                leftJoin,
                valuesC,
                ImmutableList.of(equiJoinClause(a1, c1)),
                ImmutableList.of(a1, b1, c1),
                Optional.empty());

        MultiJoinNode expected = MultiJoinNode.builder()
                .setSources(leftJoin, valuesC).setFilter(createEqualsExpression(a1, c1))
                .setOutputSymbols(a1, b1, c1)
                .build();
        assertEquals(toMultiJoinNode(joinNode, noLookup(), DEFAULT_JOIN_LIMIT), expected);
    }

    @Test
    public void testRetainsOutputSymbols()
    {
        PlanBuilder p = planBuilder();
        Symbol a1 = p.symbol("A1");
        Symbol b1 = p.symbol("B1");
        Symbol b2 = p.symbol("B2");
        Symbol c1 = p.symbol("C1");
        Symbol c2 = p.symbol("C2");
        ValuesNode valuesA = p.values(a1);
        ValuesNode valuesB = p.values(b1, b2);
        ValuesNode valuesC = p.values(c1, c2);
        JoinNode joinNode = p.join(
                INNER,
                valuesA,
                p.join(
                        INNER,
                        valuesB,
                        valuesC,
                        ImmutableList.of(equiJoinClause(b1, c1)),
                        ImmutableList.of(
                                b1,
                                b2,
                                c1,
                                c2),
                        Optional.empty()),
                ImmutableList.of(equiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1),
                Optional.empty());
        MultiJoinNode expected = MultiJoinNode.builder()
                .setSources(valuesA, valuesB, valuesC)
                .setFilter(and(createEqualsExpression(b1, c1), createEqualsExpression(a1, b1)))
                .setOutputSymbols(a1, b1)
                .build();
        assertEquals(toMultiJoinNode(joinNode, noLookup(), DEFAULT_JOIN_LIMIT), expected);
    }

    @Test
    public void testCombinesCriteriaAndFilters()
    {
        PlanBuilder p = planBuilder();
        Symbol a1 = p.symbol("A1");
        Symbol b1 = p.symbol("B1");
        Symbol b2 = p.symbol("B2");
        Symbol c1 = p.symbol("C1");
        Symbol c2 = p.symbol("C2");
        ValuesNode valuesA = p.values(a1);
        ValuesNode valuesB = p.values(b1, b2);
        ValuesNode valuesC = p.values(c1, c2);
        Expression bcFilter = and(
                new ComparisonExpression(GREATER_THAN, c2.toSymbolReference(), new LongLiteral("0")),
                new ComparisonExpression(NOT_EQUAL, c2.toSymbolReference(), new LongLiteral("7")),
                new ComparisonExpression(GREATER_THAN, b2.toSymbolReference(), c2.toSymbolReference()));
        ComparisonExpression abcFilter = new ComparisonExpression(
                LESS_THAN,
                new ArithmeticBinaryExpression(ADD, a1.toSymbolReference(), c1.toSymbolReference()),
                b1.toSymbolReference());
        JoinNode joinNode = p.join(
                INNER,
                valuesA,
                p.join(
                        INNER,
                        valuesB,
                        valuesC,
                        ImmutableList.of(equiJoinClause(b1, c1)),
                        ImmutableList.of(
                                b1,
                                b2,
                                c1,
                                c2),
                        Optional.of(bcFilter)),
                ImmutableList.of(equiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1, b2, c1, c2),
                Optional.of(abcFilter));
        MultiJoinNode expected = new MultiJoinNode(
                new LinkedHashSet<>(ImmutableList.of(valuesA, valuesB, valuesC)),
                and(new ComparisonExpression(EQUAL, b1.toSymbolReference(), c1.toSymbolReference()), new ComparisonExpression(EQUAL, a1.toSymbolReference(), b1.toSymbolReference()), bcFilter, abcFilter),
                ImmutableList.of(a1, b1, b2, c1, c2));
        assertEquals(toMultiJoinNode(joinNode, noLookup(), DEFAULT_JOIN_LIMIT), expected);
    }

    @Test
    public void testConvertsBushyTrees()
    {
        PlanBuilder p = planBuilder();
        Symbol a1 = p.symbol("A1");
        Symbol b1 = p.symbol("B1");
        Symbol c1 = p.symbol("C1");
        Symbol d1 = p.symbol("D1");
        Symbol d2 = p.symbol("D2");
        Symbol e1 = p.symbol("E1");
        Symbol e2 = p.symbol("E2");
        ValuesNode valuesA = p.values(a1);
        ValuesNode valuesB = p.values(b1);
        ValuesNode valuesC = p.values(c1);
        ValuesNode valuesD = p.values(d1, d2);
        ValuesNode valuesE = p.values(e1, e2);
        JoinNode joinNode = p.join(
                INNER,
                p.join(
                        INNER,
                        p.join(
                                INNER,
                                valuesA,
                                valuesB,
                                ImmutableList.of(equiJoinClause(a1, b1)),
                                ImmutableList.of(a1, b1),
                                Optional.empty()),
                        valuesC,
                        ImmutableList.of(equiJoinClause(a1, c1)),
                        ImmutableList.of(a1, b1, c1),
                        Optional.empty()),
                p.join(
                        INNER,
                        valuesD,
                        valuesE,
                        ImmutableList.of(
                                equiJoinClause(d1, e1),
                                equiJoinClause(d2, e2)),
                        ImmutableList.of(
                                d1,
                                d2,
                                e1,
                                e2),
                        Optional.empty()),
                ImmutableList.of(equiJoinClause(b1, e1)),
                ImmutableList.of(
                        a1,
                        b1,
                        c1,
                        d1,
                        d2,
                        e1,
                        e2),
                Optional.empty());
        MultiJoinNode expected = MultiJoinNode.builder()
                .setSources(valuesA, valuesB, valuesC, valuesD, valuesE)
                .setFilter(and(createEqualsExpression(a1, b1), createEqualsExpression(a1, c1), createEqualsExpression(d1, e1), createEqualsExpression(d2, e2), createEqualsExpression(b1, e1)))
                .setOutputSymbols(a1, b1, c1, d1, d2, e1, e2)
                .build();
        assertEquals(toMultiJoinNode(joinNode, noLookup(), 5), expected);
    }

    @Test
    public void testMoreThanJoinLimit()
    {
        PlanBuilder p = planBuilder();
        Symbol a1 = p.symbol("A1");
        Symbol b1 = p.symbol("B1");
        Symbol c1 = p.symbol("C1");
        Symbol d1 = p.symbol("D1");
        Symbol d2 = p.symbol("D2");
        Symbol e1 = p.symbol("E1");
        Symbol e2 = p.symbol("E2");
        ValuesNode valuesA = p.values(a1);
        ValuesNode valuesB = p.values(b1);
        ValuesNode valuesC = p.values(c1);
        ValuesNode valuesD = p.values(d1, d2);
        ValuesNode valuesE = p.values(e1, e2);
        JoinNode join1 = p.join(
                INNER,
                valuesA,
                valuesB,
                ImmutableList.of(equiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1),
                Optional.empty());
        JoinNode join2 = p.join(
                INNER,
                valuesD,
                valuesE,
                ImmutableList.of(
                        equiJoinClause(d1, e1),
                        equiJoinClause(d2, e2)),
                ImmutableList.of(
                        d1,
                        d2,
                        e1,
                        e2),
                Optional.empty());
        JoinNode joinNode = p.join(
                INNER,
                p.join(
                        INNER,
                        join1,
                        valuesC,
                        ImmutableList.of(equiJoinClause(a1, c1)),
                        ImmutableList.of(a1, b1, c1),
                        Optional.empty()),
                join2,
                ImmutableList.of(equiJoinClause(b1, e1)),
                ImmutableList.of(
                        a1,
                        b1,
                        c1,
                        d1,
                        d2,
                        e1,
                        e2),
                Optional.empty());
        MultiJoinNode expected = MultiJoinNode.builder()
                .setSources(join1, join2, valuesC)
                .setFilter(and(createEqualsExpression(a1, c1), createEqualsExpression(b1, e1)))
                .setOutputSymbols(a1, b1, c1, d1, d2, e1, e2)
                .build();
        assertEquals(toMultiJoinNode(joinNode, noLookup(), 2), expected);
    }

    private ComparisonExpression createEqualsExpression(Symbol left, Symbol right)
    {
        return new ComparisonExpression(EQUAL, left.toSymbolReference(), right.toSymbolReference());
    }

    private EquiJoinClause equiJoinClause(Symbol symbol1, Symbol symbol2)
    {
        return new EquiJoinClause(symbol1, symbol2);
    }

    private PlanBuilder planBuilder()
    {
        return new PlanBuilder(new PlanNodeIdAllocator(), queryRunner.getMetadata());
    }
}
