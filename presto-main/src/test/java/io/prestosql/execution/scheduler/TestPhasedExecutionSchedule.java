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
package io.prestosql.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.connector.ConnectorId;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.StageExecutionStrategy;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import io.prestosql.testing.TestingMetadata.TestingTableHandle;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.prestosql.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static org.testng.Assert.assertEquals;

public class TestPhasedExecutionSchedule
{
    @Test
    public void testExchange()
    {
        PlanFragment aFragment = createTableScanPlanFragment("a");
        PlanFragment bFragment = createTableScanPlanFragment("b");
        PlanFragment cFragment = createTableScanPlanFragment("c");
        PlanFragment exchangeFragment = createExchangePlanFragment("exchange", aFragment, bFragment, cFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(aFragment, bFragment, cFragment, exchangeFragment));
        assertEquals(phases, ImmutableList.of(
                ImmutableSet.of(exchangeFragment.getId()),
                ImmutableSet.of(aFragment.getId()),
                ImmutableSet.of(bFragment.getId()),
                ImmutableSet.of(cFragment.getId())));
    }

    @Test
    public void testUnion()
    {
        PlanFragment aFragment = createTableScanPlanFragment("a");
        PlanFragment bFragment = createTableScanPlanFragment("b");
        PlanFragment cFragment = createTableScanPlanFragment("c");
        PlanFragment unionFragment = createUnionPlanFragment("union", aFragment, bFragment, cFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(aFragment, bFragment, cFragment, unionFragment));
        assertEquals(phases, ImmutableList.of(
                ImmutableSet.of(unionFragment.getId()),
                ImmutableSet.of(aFragment.getId()),
                ImmutableSet.of(bFragment.getId()),
                ImmutableSet.of(cFragment.getId())));
    }

    @Test
    public void testJoin()
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment probeFragment = createTableScanPlanFragment("probe");
        PlanFragment joinFragment = createJoinPlanFragment(INNER, "join", buildFragment, probeFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(joinFragment, buildFragment, probeFragment));
        assertEquals(phases, ImmutableList.of(ImmutableSet.of(joinFragment.getId()), ImmutableSet.of(buildFragment.getId()), ImmutableSet.of(probeFragment.getId())));
    }

    @Test
    public void testRightJoin()
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment probeFragment = createTableScanPlanFragment("probe");
        PlanFragment joinFragment = createJoinPlanFragment(RIGHT, "join", buildFragment, probeFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(joinFragment, buildFragment, probeFragment));
        assertEquals(phases, ImmutableList.of(ImmutableSet.of(joinFragment.getId()), ImmutableSet.of(buildFragment.getId()), ImmutableSet.of(probeFragment.getId())));
    }

    @Test
    public void testBroadcastJoin()
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment joinFragment = createBroadcastJoinPlanFragment("join", buildFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(joinFragment, buildFragment));
        assertEquals(phases, ImmutableList.of(ImmutableSet.of(joinFragment.getId(), buildFragment.getId())));
    }

    @Test
    public void testJoinWithDeepSources()
    {
        PlanFragment buildSourceFragment = createTableScanPlanFragment("buildSource");
        PlanFragment buildMiddleFragment = createExchangePlanFragment("buildMiddle", buildSourceFragment);
        PlanFragment buildTopFragment = createExchangePlanFragment("buildTop", buildMiddleFragment);
        PlanFragment probeSourceFragment = createTableScanPlanFragment("probeSource");
        PlanFragment probeMiddleFragment = createExchangePlanFragment("probeMiddle", probeSourceFragment);
        PlanFragment probeTopFragment = createExchangePlanFragment("probeTop", probeMiddleFragment);
        PlanFragment joinFragment = createJoinPlanFragment(INNER, "join", buildTopFragment, probeTopFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(
                joinFragment,
                buildTopFragment,
                buildMiddleFragment,
                buildSourceFragment,
                probeTopFragment,
                probeMiddleFragment,
                probeSourceFragment));

        assertEquals(phases, ImmutableList.of(
                ImmutableSet.of(joinFragment.getId()),
                ImmutableSet.of(buildTopFragment.getId()),
                ImmutableSet.of(buildMiddleFragment.getId()),
                ImmutableSet.of(buildSourceFragment.getId()),
                ImmutableSet.of(probeTopFragment.getId()),
                ImmutableSet.of(probeMiddleFragment.getId()),
                ImmutableSet.of(probeSourceFragment.getId())));
    }

    private static PlanFragment createExchangePlanFragment(String name, PlanFragment... fragments)
    {
        PlanNode planNode = new RemoteSourceNode(
                new PlanNodeId(name + "_id"),
                Stream.of(fragments)
                        .map(PlanFragment::getId)
                        .collect(toImmutableList()),
                fragments[0].getPartitioningScheme().getOutputLayout(),
                Optional.empty(),
                REPARTITION);

        return createFragment(planNode);
    }

    private static PlanFragment createUnionPlanFragment(String name, PlanFragment... fragments)
    {
        PlanNode planNode = new UnionNode(
                new PlanNodeId(name + "_id"),
                Stream.of(fragments)
                        .map(fragment -> new RemoteSourceNode(new PlanNodeId(fragment.getId().toString()), fragment.getId(), fragment.getPartitioningScheme().getOutputLayout(), Optional.empty(), REPARTITION))
                        .collect(toImmutableList()),
                ImmutableListMultimap.of(),
                ImmutableList.of());

        return createFragment(planNode);
    }

    private static PlanFragment createBroadcastJoinPlanFragment(String name, PlanFragment buildFragment)
    {
        Symbol symbol = new Symbol("column");
        PlanNode tableScan = new TableScanNode(
                new PlanNodeId(name),
                new TableHandle(new ConnectorId("test"), new TestingTableHandle()),
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingColumnHandle("column")));

        RemoteSourceNode remote = new RemoteSourceNode(new PlanNodeId("build_id"), buildFragment.getId(), ImmutableList.of(), Optional.empty(), REPLICATE);
        PlanNode join = new JoinNode(
                new PlanNodeId(name + "_id"),
                INNER,
                tableScan,
                remote,
                ImmutableList.of(),
                ImmutableList.<Symbol>builder()
                        .addAll(tableScan.getOutputSymbols())
                        .addAll(remote.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(REPLICATED));

        return createFragment(join);
    }

    private static PlanFragment createJoinPlanFragment(JoinNode.Type joinType, String name, PlanFragment buildFragment, PlanFragment probeFragment)
    {
        RemoteSourceNode probe = new RemoteSourceNode(new PlanNodeId("probe_id"), probeFragment.getId(), ImmutableList.of(), Optional.empty(), REPARTITION);
        RemoteSourceNode build = new RemoteSourceNode(new PlanNodeId("build_id"), buildFragment.getId(), ImmutableList.of(), Optional.empty(), REPARTITION);
        PlanNode planNode = new JoinNode(
                new PlanNodeId(name + "_id"),
                joinType,
                probe,
                build,
                ImmutableList.of(),
                ImmutableList.<Symbol>builder()
                        .addAll(probe.getOutputSymbols())
                        .addAll(build.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        return createFragment(planNode);
    }

    private static PlanFragment createTableScanPlanFragment(String name)
    {
        Symbol symbol = new Symbol("column");
        PlanNode planNode = new TableScanNode(
                new PlanNodeId(name),
                new TableHandle(new ConnectorId("test"), new TestingTableHandle()),
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingColumnHandle("column")));

        return createFragment(planNode);
    }

    private static PlanFragment createFragment(PlanNode planNode)
    {
        ImmutableMap.Builder<Symbol, Type> types = ImmutableMap.builder();
        for (Symbol symbol : planNode.getOutputSymbols()) {
            types.put(symbol, VARCHAR);
        }
        return new PlanFragment(
                new PlanFragmentId(planNode.getId() + "_fragment_id"),
                planNode,
                types.build(),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputSymbols()),
                StageExecutionStrategy.ungroupedExecution(),
                StatsAndCosts.empty());
    }
}
