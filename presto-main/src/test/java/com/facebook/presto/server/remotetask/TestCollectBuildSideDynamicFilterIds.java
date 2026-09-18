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
package com.facebook.presto.server.remotetask;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link HttpRemoteTaskWithEventLoop#collectBuildSideDynamicFilterIds}.
 * Verifies that JoinNode and SemiJoinNode filter IDs are extracted correctly and that
 * probe-only fragments (no join/semijoin) yield an empty set.
 */
public class TestCollectBuildSideDynamicFilterIds
{
    private static final VariableReferenceExpression PROBE_VAR =
            new VariableReferenceExpression(Optional.empty(), "probe_col", BigintType.BIGINT);
    private static final VariableReferenceExpression BUILD_VAR =
            new VariableReferenceExpression(Optional.empty(), "build_col", BigintType.BIGINT);
    private static final VariableReferenceExpression SOURCE_VAR =
            new VariableReferenceExpression(Optional.empty(), "source_col", BigintType.BIGINT);
    private static final VariableReferenceExpression FILTERING_VAR =
            new VariableReferenceExpression(Optional.empty(), "filtering_col", BigintType.BIGINT);

    @Test
    public void testProbeOnlyFragmentYieldsEmpty()
    {
        // A bare TableScanNode has no join/semijoin — ownedFilterIds must be empty.
        // This is the probe-stage case: it has scans but produces no build-side filters.
        TableScanNode scan = createTableScan("scan_1", PROBE_VAR);
        Set<String> result = HttpRemoteTaskWithEventLoop.collectBuildSideDynamicFilterIds(scan);
        assertTrue(result.isEmpty(),
                "Probe-only fragment (TableScanNode only) must yield empty ownedFilterIds");
    }

    @Test
    public void testJoinNodeFilterIdsCollected()
    {
        // JoinNode with a dynamic filter: the filter ID must be collected.
        String filterId = "filter_join_1";
        RemoteSourceNode probeSource = createRemoteSource("probe_src", 1, PROBE_VAR);
        RemoteSourceNode buildSource = createRemoteSource("build_src", 2, BUILD_VAR);
        JoinNode join = new JoinNode(
                Optional.empty(),
                new PlanNodeId("join_1"),
                INNER,
                probeSource,
                buildSource,
                ImmutableList.of(new EquiJoinClause(PROBE_VAR, BUILD_VAR)),
                ImmutableList.of(PROBE_VAR, BUILD_VAR),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(filterId, BUILD_VAR));

        Set<String> result = HttpRemoteTaskWithEventLoop.collectBuildSideDynamicFilterIds(join);
        assertEquals(result, ImmutableSet.of(filterId),
                "JoinNode dynamic filter ID must be collected");
    }

    @Test
    public void testSemiJoinNodeFilterIdsCollected()
    {
        // SemiJoinNode with a dynamic filter: the filter ID must be collected.
        // Use ValuesNode for source/filteringSource to satisfy SemiJoinNode's
        // checkArgument that source/filteringSource output variables contain the join variables.
        String filterId = "filter_semi_1";
        ValuesNode source = new ValuesNode(
                Optional.empty(), new PlanNodeId("source_values"),
                ImmutableList.of(SOURCE_VAR), ImmutableList.of(), Optional.empty());
        ValuesNode filteringSource = new ValuesNode(
                Optional.empty(), new PlanNodeId("filtering_values"),
                ImmutableList.of(FILTERING_VAR), ImmutableList.of(), Optional.empty());
        VariableReferenceExpression semiOutput =
                new VariableReferenceExpression(Optional.empty(), "semi_output", BigintType.BIGINT);
        SemiJoinNode semiJoin = new SemiJoinNode(
                Optional.empty(),
                new PlanNodeId("semi_join_1"),
                source,
                filteringSource,
                SOURCE_VAR,
                FILTERING_VAR,
                semiOutput,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(filterId, SOURCE_VAR));

        Set<String> result = HttpRemoteTaskWithEventLoop.collectBuildSideDynamicFilterIds(semiJoin);
        assertEquals(result, ImmutableSet.of(filterId),
                "SemiJoinNode dynamic filter ID must be collected");
    }

    @Test
    public void testMultipleJoinsAllFilterIdsCollected()
    {
        // Two nested joins each with their own filter ID: both must be collected.
        String outerFilterId = "filter_outer";
        String innerFilterId = "filter_inner";

        RemoteSourceNode remoteSource1 = createRemoteSource("src_1", 1, PROBE_VAR);
        RemoteSourceNode remoteSource2 = createRemoteSource("src_2", 2, BUILD_VAR);
        RemoteSourceNode remoteSource3 = createRemoteSource("src_3", 3, BUILD_VAR);

        JoinNode innerJoin = new JoinNode(
                Optional.empty(),
                new PlanNodeId("inner_join"),
                INNER,
                remoteSource1,
                remoteSource2,
                ImmutableList.of(new EquiJoinClause(PROBE_VAR, BUILD_VAR)),
                ImmutableList.of(PROBE_VAR, BUILD_VAR),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(innerFilterId, BUILD_VAR));

        JoinNode outerJoin = new JoinNode(
                Optional.empty(),
                new PlanNodeId("outer_join"),
                INNER,
                innerJoin,
                remoteSource3,
                ImmutableList.of(new EquiJoinClause(PROBE_VAR, BUILD_VAR)),
                ImmutableList.of(PROBE_VAR, BUILD_VAR),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(outerFilterId, BUILD_VAR));

        Set<String> result = HttpRemoteTaskWithEventLoop.collectBuildSideDynamicFilterIds(outerJoin);
        assertEquals(result, ImmutableSet.of(outerFilterId, innerFilterId),
                "All JoinNode dynamic filter IDs must be collected from the full subtree");
    }

    private static TableScanNode createTableScan(String id, VariableReferenceExpression variable)
    {
        return new TableScanNode(
                Optional.empty(),
                new PlanNodeId(id),
                new TableHandle(new ConnectorId("test"), new TestingTableHandle(),
                        TestingTransactionHandle.create(), Optional.empty()),
                ImmutableList.of(variable),
                ImmutableMap.of(variable, new TestingColumnHandle(variable.getName())),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
    }

    private static RemoteSourceNode createRemoteSource(String id, int fragmentId, VariableReferenceExpression variable)
    {
        return new RemoteSourceNode(
                Optional.empty(),
                new PlanNodeId(id),
                new com.facebook.presto.spi.plan.PlanFragmentId(fragmentId),
                ImmutableList.of(variable),
                false,
                Optional.empty(),
                REPARTITION);
    }
}
