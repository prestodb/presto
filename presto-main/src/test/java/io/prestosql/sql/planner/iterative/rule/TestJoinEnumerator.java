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
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.cost.CachingCostProvider;
import io.prestosql.cost.CachingStatsProvider;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.CostProvider;
import io.prestosql.cost.PlanNodeCostEstimate;
import io.prestosql.cost.StatsProvider;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult;
import io.prestosql.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator;
import io.prestosql.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.prestosql.sql.planner.iterative.Lookup.noLookup;
import static io.prestosql.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator.generatePartitions;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestJoinEnumerator
{
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
        PlanBuilder p = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        Symbol a1 = p.symbol("A1");
        Symbol b1 = p.symbol("B1");
        MultiJoinNode multiJoinNode = new MultiJoinNode(
                new LinkedHashSet<>(ImmutableList.of(p.values(a1), p.values(b1))),
                TRUE_LITERAL,
                ImmutableList.of(a1, b1));
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                new CostComparator(1, 1, 1),
                multiJoinNode.getFilter(),
                createContext());
        JoinEnumerationResult actual = joinEnumerator.createJoinAccordingToPartitioning(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols(), ImmutableSet.of(0));
        assertFalse(actual.getPlanNode().isPresent());
        assertEquals(actual.getCost(), PlanNodeCostEstimate.infinite());
    }

    private Rule.Context createContext()
    {
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        CachingStatsProvider statsProvider = new CachingStatsProvider(
                queryRunner.getStatsCalculator(),
                Optional.empty(),
                noLookup(),
                queryRunner.getDefaultSession(),
                symbolAllocator.getTypes());
        CachingCostProvider costProvider = new CachingCostProvider(
                queryRunner.getCostCalculator(),
                statsProvider,
                Optional.empty(),
                queryRunner.getDefaultSession(),
                symbolAllocator.getTypes());

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
            public SymbolAllocator getSymbolAllocator()
            {
                return symbolAllocator;
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
        };
    }
}
