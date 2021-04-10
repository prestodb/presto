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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode.toMultiJoinNode;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestJoinNodeFlattener
{
    private static final int DEFAULT_JOIN_LIMIT = 10;
    private DeterminismEvaluator determinismEvaluator;
    private FunctionResolution functionResolution;

    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = new LocalQueryRunner(testSessionBuilder().build());
        determinismEvaluator = new RowExpressionDeterminismEvaluator(queryRunner.getMetadata());
        functionResolution = new FunctionResolution(queryRunner.getMetadata().getFunctionAndTypeManager());
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
        VariableReferenceExpression a1 = p.variable("A1");
        VariableReferenceExpression b1 = p.variable("B1");
        JoinNode outerJoin = p.join(
                FULL,
                p.values(a1),
                p.values(b1),
                ImmutableList.of(equiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1),
                Optional.empty());
        toMultiJoinNode(outerJoin, noLookup(), DEFAULT_JOIN_LIMIT, functionResolution, determinismEvaluator);
    }

    @Test
    public void testDoesNotConvertNestedOuterJoins()
    {
        PlanBuilder p = planBuilder();
        VariableReferenceExpression a1 = p.variable("A1");
        VariableReferenceExpression b1 = p.variable("B1");
        VariableReferenceExpression c1 = p.variable("C1");
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
                .setOutputVariables(a1, b1, c1)
                .build();
        assertEquals(toMultiJoinNode(joinNode, noLookup(), DEFAULT_JOIN_LIMIT, functionResolution, determinismEvaluator), expected);
    }

    @Test
    public void testRetainsOutputSymbols()
    {
        PlanBuilder p = planBuilder();
        VariableReferenceExpression a1 = p.variable("A1");
        VariableReferenceExpression b1 = p.variable("B1");
        VariableReferenceExpression b2 = p.variable("B2");
        VariableReferenceExpression c1 = p.variable("C1");
        VariableReferenceExpression c2 = p.variable("C2");
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
                        ImmutableList.of(b1, b2, c1, c2),
                        Optional.empty()),
                ImmutableList.of(equiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1),
                Optional.empty());
        MultiJoinNode expected = MultiJoinNode.builder()
                .setSources(valuesA, valuesB, valuesC)
                .setFilter(and(createEqualsExpression(b1, c1), createEqualsExpression(a1, b1)))
                .setOutputVariables(a1, b1)
                .build();
        assertEquals(toMultiJoinNode(joinNode, noLookup(), DEFAULT_JOIN_LIMIT, functionResolution, determinismEvaluator), expected);
    }

    @Test
    public void testCombinesCriteriaAndFilters()
    {
        PlanBuilder p = planBuilder();
        VariableReferenceExpression a1 = p.variable("A1");
        VariableReferenceExpression b1 = p.variable("B1");
        VariableReferenceExpression b2 = p.variable("B2");
        VariableReferenceExpression c1 = p.variable("C1");
        VariableReferenceExpression c2 = p.variable("C2");
        ValuesNode valuesA = p.values(a1);
        ValuesNode valuesB = p.values(b1, b2);
        ValuesNode valuesC = p.values(c1, c2);
        RowExpression bcFilter = and(
                call(
                        OperatorType.GREATER_THAN.name(),
                        functionResolution.comparisonFunction(OperatorType.GREATER_THAN, c2.getType(), BIGINT),
                        BOOLEAN,
                        ImmutableList.of(c2, constant(0L, BIGINT))),
                call(
                        OperatorType.NOT_EQUAL.name(),
                        functionResolution.comparisonFunction(OperatorType.NOT_EQUAL, c2.getType(), BIGINT),
                        BOOLEAN,
                        ImmutableList.of(c2, constant(7L, BIGINT))),
                call(
                        OperatorType.GREATER_THAN.name(),
                        functionResolution.comparisonFunction(OperatorType.GREATER_THAN, b2.getType(), c2.getType()),
                        BOOLEAN,
                        ImmutableList.of(b2, c2)));
        RowExpression add = call(
                OperatorType.ADD.name(),
                functionResolution.arithmeticFunction(OperatorType.ADD, a1.getType(), c1.getType()),
                a1.getType(),
                ImmutableList.of(a1, c1));
        RowExpression abcFilter = call(
                OperatorType.LESS_THAN.name(),
                functionResolution.comparisonFunction(OperatorType.LESS_THAN, add.getType(), b1.getType()),
                BOOLEAN,
                ImmutableList.of(add, b1));
        JoinNode joinNode = p.join(
                INNER,
                valuesA,
                p.join(
                        INNER,
                        valuesB,
                        valuesC,
                        ImmutableList.of(equiJoinClause(b1, c1)),
                        ImmutableList.of(b1, b2, c1, c2),
                        Optional.of(bcFilter)),
                ImmutableList.of(equiJoinClause(a1, b1)),
                ImmutableList.of(a1, b1, b2, c1, c2),
                Optional.of(abcFilter));
        MultiJoinNode expected = new MultiJoinNode(
                new LinkedHashSet<>(ImmutableList.of(valuesA, valuesB, valuesC)),
                and(createEqualsExpression(b1, c1), createEqualsExpression(a1, b1), bcFilter, abcFilter),
                ImmutableList.of(a1, b1, b2, c1, c2));
        assertEquals(toMultiJoinNode(joinNode, noLookup(), DEFAULT_JOIN_LIMIT, functionResolution, determinismEvaluator), expected);
    }

    @Test
    public void testConvertsBushyTrees()
    {
        PlanBuilder p = planBuilder();
        VariableReferenceExpression a1 = p.variable("A1");
        VariableReferenceExpression b1 = p.variable("B1");
        VariableReferenceExpression c1 = p.variable("C1");
        VariableReferenceExpression d1 = p.variable("D1");
        VariableReferenceExpression d2 = p.variable("D2");
        VariableReferenceExpression e1 = p.variable("E1");
        VariableReferenceExpression e2 = p.variable("E2");
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
                        ImmutableList.of(d1, d2, e1, e2),
                        Optional.empty()),
                ImmutableList.of(equiJoinClause(b1, e1)),
                ImmutableList.of(a1, b1, c1, d1, d2, e1, e2),
                Optional.empty());
        MultiJoinNode expected = MultiJoinNode.builder()
                .setSources(valuesA, valuesB, valuesC, valuesD, valuesE)
                .setFilter(and(createEqualsExpression(a1, b1), createEqualsExpression(a1, c1), createEqualsExpression(d1, e1), createEqualsExpression(d2, e2), createEqualsExpression(b1, e1)))
                .setOutputVariables(a1, b1, c1, d1, d2, e1, e2)
                .build();
        assertEquals(toMultiJoinNode(joinNode, noLookup(), 5, functionResolution, determinismEvaluator), expected);
    }

    @Test
    public void testMoreThanJoinLimit()
    {
        PlanBuilder p = planBuilder();
        VariableReferenceExpression a1 = p.variable("A1");
        VariableReferenceExpression b1 = p.variable("B1");
        VariableReferenceExpression c1 = p.variable("C1");
        VariableReferenceExpression d1 = p.variable("D1");
        VariableReferenceExpression d2 = p.variable("D2");
        VariableReferenceExpression e1 = p.variable("E1");
        VariableReferenceExpression e2 = p.variable("E2");
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
                ImmutableList.of(d1, d2, e1, e2),
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
                ImmutableList.of(a1, b1, c1, d1, d2, e1, e2),
                Optional.empty());
        MultiJoinNode expected = MultiJoinNode.builder()
                .setSources(join1, join2, valuesC)
                .setFilter(and(createEqualsExpression(a1, c1), createEqualsExpression(b1, e1)))
                .setOutputVariables(a1, b1, c1, d1, d2, e1, e2)
                .build();
        assertEquals(toMultiJoinNode(joinNode, noLookup(), 2, functionResolution, determinismEvaluator), expected);
    }

    private RowExpression createEqualsExpression(VariableReferenceExpression left, VariableReferenceExpression right)
    {
        return call(
                OperatorType.EQUAL.name(),
                functionResolution.comparisonFunction(OperatorType.EQUAL, left.getType(), right.getType()),
                BOOLEAN,
                ImmutableList.of(left, right));
    }

    private EquiJoinClause equiJoinClause(VariableReferenceExpression variable1, VariableReferenceExpression variable2)
    {
        return new EquiJoinClause(variable1, variable2);
    }

    private PlanBuilder planBuilder()
    {
        return new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), queryRunner.getMetadata());
    }
}
