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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.tpch.TpchPartitioningHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.StageExecutionStrategy.ungroupedExecution;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class TestFragmentedPlanStatsCalculator
{
    private Metadata metadata;
    private Session session;

    @BeforeClass
    public void setUp()
    {
        this.metadata = createTestMetadataManager();
        this.session = testSessionBuilder().build();
    }

    @AfterClass
    public void tearDown()
    {
        this.metadata = null;
        this.session = null;
    }

    @Test
    public void testRemoteSourceNodeStats()
    {
        PlanBuilder pb = new PlanBuilder(new PlanNodeIdAllocator(), metadata);

        PlanNode values1 = pb.values(pb.symbol("i11", BIGINT), pb.symbol("i12", BIGINT), pb.symbol("i13", BIGINT), pb.symbol("i14", BIGINT));
        PlanFragment fragment1 = planFragment("f1", values1, pb.getTypes().allTypes(), values1.getOutputSymbols());
        PlanNodeStatsEstimate values1Stats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10)
                .addSymbolStatistics(new Symbol("i11"), SymbolStatsEstimate.builder()
                        .setLowValue(1)
                        .setHighValue(10)
                        .setDistinctValuesCount(5)
                        .setNullsFraction(0.3)
                        .build())
                .addSymbolStatistics(new Symbol("i12"), SymbolStatsEstimate.builder()
                        .setLowValue(0)
                        .setHighValue(3)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0)
                        .build())
                .addSymbolStatistics(new Symbol("i13"), SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .build())
                .addSymbolStatistics(new Symbol("i14"), SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .build())
                .build();

        PlanNode values2 = pb.values(pb.symbol("i21", BIGINT), pb.symbol("i22", BIGINT), pb.symbol("i23", BIGINT), pb.symbol("i24", BIGINT));
        PlanFragment fragment2 = planFragment("f2", values2, pb.getTypes().allTypes(), values2.getOutputSymbols());
        PlanNodeStatsEstimate values2Stats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(20)
                .addSymbolStatistics(new Symbol("i21"), SymbolStatsEstimate.builder()
                        .setLowValue(11)
                        .setHighValue(20)
                        .setNullsFraction(0.4)
                        .build())
                .addSymbolStatistics(new Symbol("i22"), SymbolStatsEstimate.builder()
                        .setLowValue(2)
                        .setHighValue(7)
                        .setDistinctValuesCount(3)
                        .build())
                .addSymbolStatistics(new Symbol("i23"), SymbolStatsEstimate.builder()
                        .setDistinctValuesCount(6)
                        .setNullsFraction(0.2)
                        .build())
                .addSymbolStatistics(new Symbol("i24"), SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .build())
                .build();

        Map<PlanNode, PlanNodeStatsEstimate> stats = ImmutableMap.<PlanNode, PlanNodeStatsEstimate>builder()
                .put(values1, values1Stats)
                .put(values2, values2Stats)
                .build();

        List<PlanFragment> fragments = ImmutableList.of(fragment1, fragment2);
        ImmutableList<PlanFragmentId> fragmentIds = fragments.stream().map(PlanFragment::getId).collect(toImmutableList());
        PlanNode remoteSourceNode = pb.remoteSourceNode(fragmentIds,
                ImmutableList.of(pb.symbol("o1", BIGINT),
                        pb.symbol("o2", BIGINT),
                        pb.symbol("o3", BIGINT),
                        pb.symbol("o4", BIGINT)),
                REPARTITION);

        assertStatsFor(remoteSourceNode, stats, fragments, pb.getTypes())
                .check(check -> check
                        .outputRowsCount(30)
                        .symbolStats("o1", assertion -> assertion
                                .lowValue(1)
                                .highValue(20)
                                .distinctValuesCount(5)
                                .nullsFraction(0.3666666))
                        .symbolStats("o2", assertion -> assertion
                                .lowValue(0)
                                .highValue(7)
                                .distinctValuesCount(3)
                                .nullsFractionUnknown())
                        .symbolStats("o3", assertion -> assertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCount(4.0)
                                .nullsFraction(0.1666667))
                        .symbolStats("o4", assertion -> assertion
                                .lowValue(10)
                                .highValue(15)
                                .distinctValuesCount(4)
                                .nullsFraction(0.1)));
    }

    private PlanFragment planFragment(String id, PlanNode root, Map<Symbol, Type> types, List<Symbol> outputLayout)
    {
        PartitioningHandle partitioningHandle = new PartitioningHandle(Optional.empty(), Optional.empty(), new TpchPartitioningHandle("dummy", 100));
        return new PlanFragment(new PlanFragmentId(id),
                root,
                types,
                partitioningHandle,
                emptyList(),
                new PartitioningScheme(Partitioning.create(partitioningHandle, emptyList()), outputLayout),
                ungroupedExecution());
    }

    private StatsCalculatorAssertion assertStatsFor(PlanNode node, Map<PlanNode, PlanNodeStatsEstimate> stats, List<PlanFragment> fragments, TypeProvider types)
    {
        StatsCalculator delegate = statsCalculator(stats);
        FragmentedPlanSourceProvider sourceProvider = FragmentedPlanSourceProvider.create(fragments);
        FragmentedPlanStatsCalculator statsCalculator = new FragmentedPlanStatsCalculator(delegate, sourceProvider);

        return new StatsCalculatorAssertion(statsCalculator, session, node, types)
                .withSourceStats(stats);
    }

    private StatsCalculator statsCalculator(Map<PlanNode, PlanNodeStatsEstimate> stats)
    {
        return (node, sourceStats, lookup, session, types) ->
                requireNonNull(stats.get(node), "no stats for node");
    }
}
