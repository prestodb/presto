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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.CachingCostProvider;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.LogicalPropertiesProvider;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.RowExpressionVerifier;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult;
import com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator;
import com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator.JoinCondition;
import com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator.generatePartitions;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.optimizations.JoinNodeUtils.toRowExpression;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestJoinEnumerator
{
    private LocalQueryRunner queryRunner;
    private Metadata metadata;
    private DeterminismEvaluator determinismEvaluator;
    private FunctionResolution functionResolution;
    private PlanBuilder planBuilder;
    private TestingRowExpressionTranslator rowExpressionTranslator;
    private Session session;

    @BeforeClass
    public void setUp()
    {
        session = testSessionBuilder().build();
        queryRunner = new LocalQueryRunner(session);
        metadata = queryRunner.getMetadata();
        determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata);
        functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
        planBuilder = new PlanBuilder(session, new PlanNodeIdAllocator(), metadata);
        rowExpressionTranslator = new TestingRowExpressionTranslator(metadata);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    public void testGeneratePartitions()
    {
        assertEquals(generatePartitions(4),
                ImmutableSet.of(
                        ImmutableSet.of(0),
                        ImmutableSet.of(0, 1),
                        ImmutableSet.of(0, 2),
                        ImmutableSet.of(0, 3),
                        ImmutableSet.of(0, 1, 2),
                        ImmutableSet.of(0, 1, 3),
                        ImmutableSet.of(0, 2, 3)));

        assertEquals(generatePartitions(3),
                ImmutableSet.of(
                        ImmutableSet.of(0),
                        ImmutableSet.of(0, 1),
                        ImmutableSet.of(0, 2)));
    }

    @Test
    public void testDoesNotCreateJoinWhenPartitionedOnCrossJoin()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder p = new PlanBuilder(TEST_SESSION, idAllocator, queryRunner.getMetadata());
        VariableReferenceExpression a1 = p.variable("A1");
        VariableReferenceExpression b1 = p.variable("B1");
        MultiJoinNode multiJoinNode = new MultiJoinNode(
                new LinkedHashSet<>(ImmutableList.of(p.values(a1), p.values(b1))),
                TRUE_CONSTANT,
                ImmutableList.of(a1, b1),
                Assignments.of());
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                new CostComparator(1, 1, 1),
                multiJoinNode.getFilter(),
                createContext(),
                determinismEvaluator,
                functionResolution,
                metadata);
        JoinEnumerationResult actual = joinEnumerator.createJoinAccordingToPartitioning(multiJoinNode.getSources(), multiJoinNode.getOutputVariables(), ImmutableSet.of(0));
        assertFalse(actual.getPlanNode().isPresent());
        assertEquals(actual.getCost(), PlanCostEstimate.infinite());
    }

    @Test
    public void testJoinClauseAndFilterInference()
    {
        ImmutableMap.Builder<String, Type> builder = ImmutableMap.builder();
        builder.put("a", BIGINT);
        builder.put("b", BIGINT);
        builder.put("c", BIGINT);
        builder.put("d", BIGINT);
        Map<String, Type> variableMap = builder.build();
        VariableReferenceExpression a = variable("a", variableMap.get("a"));
        VariableReferenceExpression b = variable("b", variableMap.get("b"));
        VariableReferenceExpression c = variable("c", variableMap.get("c"));
        VariableReferenceExpression d = variable("d", variableMap.get("d"));

        SymbolAliases.Builder newAliases = SymbolAliases.builder();
        newAliases.put("A", new SymbolReference("a"));
        newAliases.put("B", new SymbolReference("b"));
        newAliases.put("C", new SymbolReference("c"));
        newAliases.put("D", new SymbolReference("d"));
        SymbolAliases symbolAliases = newAliases.build();

        // Simple join predicates on variable references
        assertJoinCondition(symbolAliases, toRowExpressionList(variableMap, "a = b"), ImmutableSet.of(a), ImmutableSet.of(b, c), "A = B", null);
        assertJoinCondition(symbolAliases, toRowExpressionList(variableMap, "a = b", "c = d"), ImmutableSet.of(a, c), ImmutableSet.of(b, d), "A = B AND C = D", null);
        // Complex join predicate - All variables must be from one join side to have the predicate be an equi-join clause
        assertJoinCondition(symbolAliases, toRowExpressionList(variableMap, "a = b + c"), ImmutableSet.of(a), ImmutableSet.of(b, c), "A = B + C", null);
        // Left and right side designation can be switched
        assertJoinCondition(symbolAliases, toRowExpressionList(variableMap, "a = b + c"), ImmutableSet.of(b, c), ImmutableSet.of(a), "A = B + C", null);
        assertJoinCondition(symbolAliases, toRowExpressionList(variableMap, "a = b + c + 1"), ImmutableSet.of(a), ImmutableSet.of(b, c), "A = B + C + 1", null);
        assertJoinCondition(symbolAliases, toRowExpressionList(variableMap, "a = b + c + 1"), ImmutableSet.of(b, c), ImmutableSet.of(a), "A = B + C + 1", null);
        // If a predicate has a mix of variables from left & right sides, the predicate is treated as a filter
        assertJoinCondition(symbolAliases, toRowExpressionList(variableMap, "a + b = c"), ImmutableSet.of(a), ImmutableSet.of(b, c), null, "A + B = C");
        assertJoinCondition(symbolAliases, toRowExpressionList(variableMap, "a + b = 1"), ImmutableSet.of(a), ImmutableSet.of(b), null, "A + B = 1");
        // Test with multiple equi-join conditions and filters
        assertJoinCondition(symbolAliases, toRowExpressionList(variableMap, "a = ABS(b)", "a = ceil(b-c)", "b = c + 10"),
                ImmutableSet.of(a), ImmutableSet.of(b, c), "A = abs(B) AND A = ceil(B-C)", "B = C + 10");
    }

    private List<RowExpression> toRowExpressionList(Map<String, Type> variableTypeMap, String... predicates)
    {
        return Arrays.stream(predicates)
                .map(p -> rowExpressionTranslator.translate(p, variableTypeMap))
                .collect(Collectors.toList());
    }

    private void assertJoinCondition(SymbolAliases symbolAliases, List<RowExpression> joinPredicates, Set<VariableReferenceExpression> leftVariables,
            Set<VariableReferenceExpression> rightVariables, String expectedEquiJoinClause, String expectedJoinFilter)
    {
        RowExpressionVerifier verifier = new RowExpressionVerifier(symbolAliases, metadata, session);
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                new CostComparator(1, 1, 1),
                TRUE_CONSTANT,
                createContext(),
                determinismEvaluator,
                functionResolution,
                metadata);

        JoinCondition joinConditions = joinEnumerator.extractJoinConditions(joinPredicates,
                leftVariables, rightVariables, new VariableAllocator());

        Optional<RowExpression> equiJoinExpressionInlined = joinConditions.getJoinClauses().stream()
                .map(criteria -> toRowExpression(criteria, functionResolution))
                // We may have made left or right assignments to build the equi join clause
                // We inline these assignments for building the equi join clause to verify
                .map(expression -> replaceExpression(expression, joinConditions.getNewLeftAssignments()))
                .map(expression -> replaceExpression(expression, joinConditions.getNewRightAssignments()))
                .reduce(LogicalRowExpressions::and);

        if (equiJoinExpressionInlined.isPresent()) {
            assertNotNull(expectedEquiJoinClause);
            assertTrue(verifier.process(expression(expectedEquiJoinClause), equiJoinExpressionInlined.get()));
        }
        else {
            assertNull(expectedEquiJoinClause);
        }

        Optional<RowExpression> joinFilter = joinConditions.getJoinFilters().stream()
                .reduce(LogicalRowExpressions::and);

        if (joinFilter.isPresent()) {
            assertNotNull(expectedJoinFilter);
            assertTrue(verifier.process(expression(expectedJoinFilter), joinFilter.get()));
        }
        else {
            assertNull(expectedJoinFilter);
        }
    }

    private Rule.Context createContext()
    {
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();
        CachingStatsProvider statsProvider = new CachingStatsProvider(
                queryRunner.getStatsCalculator(),
                Optional.empty(),
                noLookup(),
                queryRunner.getDefaultSession(),
                TypeProvider.viewOf(variableAllocator.getVariables()));
        CachingCostProvider costProvider = new CachingCostProvider(
                queryRunner.getCostCalculator(),
                statsProvider,
                Optional.empty(),
                queryRunner.getDefaultSession());

        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return noLookup();
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return planNodeIdAllocator;
            }

            @Override
            public VariableAllocator getVariableAllocator()
            {
                return variableAllocator;
            }

            @Override
            public Session getSession()
            {
                return queryRunner.getDefaultSession();
            }

            @Override
            public StatsProvider getStatsProvider()
            {
                return statsProvider;
            }

            @Override
            public CostProvider getCostProvider()
            {
                return costProvider;
            }

            @Override
            public void checkTimeoutNotExhausted() {}

            @Override
            public WarningCollector getWarningCollector()
            {
                return WarningCollector.NOOP;
            }

            @Override
            public Optional<LogicalPropertiesProvider> getLogicalPropertiesProvider()
            {
                return Optional.empty();
            }
        };
    }
}
