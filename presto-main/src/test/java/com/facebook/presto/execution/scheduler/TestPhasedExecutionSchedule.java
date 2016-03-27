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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PartitionFunctionBinding;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestPhasedExecutionSchedule
{
    @Test
    public void testExchange()
            throws Exception
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
            throws Exception
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
            throws Exception
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment probeFragment = createTableScanPlanFragment("probe");
        PlanFragment joinFragment = createJoinPlanFragment(INNER, "join", buildFragment, probeFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(joinFragment, buildFragment, probeFragment));
        assertEquals(phases, ImmutableList.of(ImmutableSet.of(joinFragment.getId()), ImmutableSet.of(buildFragment.getId()), ImmutableSet.of(probeFragment.getId())));
    }

    @Test
    public void testRightJoin()
            throws Exception
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment probeFragment = createTableScanPlanFragment("probe");
        PlanFragment joinFragment = createJoinPlanFragment(RIGHT, "join", buildFragment, probeFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(joinFragment, buildFragment, probeFragment));
        assertEquals(phases, ImmutableList.of(ImmutableSet.of(joinFragment.getId()), ImmutableSet.of(buildFragment.getId()), ImmutableSet.of(probeFragment.getId())));
    }

    @Test
    public void testBroadcastJoin()
            throws Exception
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment joinFragment = createBroadcastJoinPlanFragment("join", buildFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(joinFragment, buildFragment));
        assertEquals(phases, ImmutableList.of(ImmutableSet.of(joinFragment.getId(), buildFragment.getId())));
    }

    @Test
    public void testJoinWithDeepSources()
            throws Exception
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
                fragments[0].getPartitionFunction().getOutputLayout());

        return createFragment(planNode);
    }

    private static PlanFragment createUnionPlanFragment(String name, PlanFragment... fragments)
    {
        PlanNode planNode = new UnionNode(
                new PlanNodeId(name + "_id"),
                Stream.of(fragments)
                        .map(fragment -> new RemoteSourceNode(new PlanNodeId(fragment.getId().toString()), fragment.getId(), fragment.getPartitionFunction().getOutputLayout()))
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
                new TableHandle("test", new TestingTableHandle()),
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingColumnHandle("column")),
                Optional.empty(),
                TupleDomain.all(),
                null);

        PlanNode join = new JoinNode(
                new PlanNodeId(name + "_id"),
                INNER,
                tableScan,
                new RemoteSourceNode(new PlanNodeId("build_id"), buildFragment.getId(), ImmutableList.of()),
                ImmutableList.of(),
                Optional.<Symbol>empty(),
                Optional.<Symbol>empty());

        return createFragment(join);
    }

    private static PlanFragment createJoinPlanFragment(JoinNode.Type joinType, String name, PlanFragment buildFragment, PlanFragment probeFragment)
    {
        PlanNode planNode = new JoinNode(
                new PlanNodeId(name + "_id"),
                joinType,
                new RemoteSourceNode(new PlanNodeId("probe_id"), probeFragment.getId(), ImmutableList.of()),
                new RemoteSourceNode(new PlanNodeId("build_id"), buildFragment.getId(), ImmutableList.of()),
                ImmutableList.of(),
                Optional.<Symbol>empty(),
                Optional.<Symbol>empty());

        return createFragment(planNode);
    }

    private static PlanFragment createTableScanPlanFragment(String name)
    {
        Symbol symbol = new Symbol("column");
        PlanNode planNode = new TableScanNode(
                new PlanNodeId(name),
                new TableHandle("test", new TestingTableHandle()),
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingColumnHandle("column")),
                Optional.empty(),
                TupleDomain.all(),
                null);

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
                planNode.getId(),
                new PartitionFunctionBinding(SINGLE_DISTRIBUTION, planNode.getOutputSymbols(), ImmutableList.of()));
    }
}
