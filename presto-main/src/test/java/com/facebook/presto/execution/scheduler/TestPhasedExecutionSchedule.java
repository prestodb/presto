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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestPhasedExecutionSchedule
{
    private static final AtomicInteger nextPlanFragmentId = new AtomicInteger();

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
                false,
                Optional.empty(),
                REPARTITION);

        return createFragment(planNode);
    }

    private static PlanFragment createUnionPlanFragment(String name, PlanFragment... fragments)
    {
        PlanNode planNode = new UnionNode(
                new PlanNodeId(name + "_id"),
                Stream.of(fragments)
                        .map(fragment -> new RemoteSourceNode(
                                new PlanNodeId(fragment.getId().toString()),
                                fragment.getId(),
                                fragment.getPartitioningScheme().getOutputLayout(),
                                false,
                                Optional.empty(),
                                REPARTITION))
                        .collect(toImmutableList()),
                ImmutableList.of(),
                ImmutableMap.of());

        return createFragment(planNode);
    }

    private static PlanFragment createBroadcastJoinPlanFragment(String name, PlanFragment buildFragment)
    {
        VariableReferenceExpression variable = new VariableReferenceExpression("column", BIGINT);
        PlanNode tableScan = new TableScanNode(
                new PlanNodeId(name),
                new TableHandle(
                        new ConnectorId("test"),
                        new TestingTableHandle(),
                        TestingTransactionHandle.create(),
                        Optional.empty()),
                ImmutableList.of(variable),
                ImmutableMap.of(variable, new TestingColumnHandle("column")),
                TupleDomain.all(),
                TupleDomain.all());

        RemoteSourceNode remote = new RemoteSourceNode(new PlanNodeId("build_id"), buildFragment.getId(), ImmutableList.of(), false, Optional.empty(), REPLICATE);
        PlanNode join = new JoinNode(
                new PlanNodeId(name + "_id"),
                INNER,
                tableScan,
                remote,
                ImmutableList.of(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(tableScan.getOutputVariables())
                        .addAll(remote.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(REPLICATED),
                ImmutableMap.of());

        return createFragment(join);
    }

    private static PlanFragment createJoinPlanFragment(JoinNode.Type joinType, String name, PlanFragment buildFragment, PlanFragment probeFragment)
    {
        RemoteSourceNode probe = new RemoteSourceNode(new PlanNodeId("probe_id"), probeFragment.getId(), ImmutableList.of(), false, Optional.empty(), REPARTITION);
        RemoteSourceNode build = new RemoteSourceNode(new PlanNodeId("build_id"), buildFragment.getId(), ImmutableList.of(), false, Optional.empty(), REPARTITION);
        PlanNode planNode = new JoinNode(
                new PlanNodeId(name + "_id"),
                joinType,
                probe,
                build,
                ImmutableList.of(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(probe.getOutputVariables())
                        .addAll(build.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
        return createFragment(planNode);
    }

    private static PlanFragment createTableScanPlanFragment(String name)
    {
        VariableReferenceExpression variable = new VariableReferenceExpression("column", BIGINT);
        PlanNode planNode = new TableScanNode(
                new PlanNodeId(name),
                new TableHandle(
                        new ConnectorId("test"),
                        new TestingTableHandle(),
                        TestingTransactionHandle.create(),
                        Optional.empty()),
                ImmutableList.of(variable),
                ImmutableMap.of(variable, new TestingColumnHandle("column")),
                TupleDomain.all(),
                TupleDomain.all());

        return createFragment(planNode);
    }

    private static PlanFragment createFragment(PlanNode planNode)
    {
        return new PlanFragment(
                new PlanFragmentId(nextPlanFragmentId.incrementAndGet()),
                planNode,
                ImmutableSet.copyOf(planNode.getOutputVariables()),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputVariables()),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());
    }
}
