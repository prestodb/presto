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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.metadata.TableFunctionHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.split.SplitSourceProvider;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME;
import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_STRATEGY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDynamicFilterService
{
    private static final Duration DEFAULT_TIMEOUT = new Duration(2, TimeUnit.SECONDS);

    private DynamicFilterService service;

    @BeforeMethod
    public void setUp()
    {
        service = new DynamicFilterService();
    }

    @Test
    public void testRegisterAndGet()
    {
        QueryId queryId = QueryId.valueOf("test_query_1");
        String filterId = "filter_1";
        JoinDynamicFilter filter = createTestFilter();

        service.registerFilter(queryId, filterId, filter);

        Optional<JoinDynamicFilter> retrieved = service.getFilter(queryId, filterId);
        assertTrue(retrieved.isPresent());
        assertEquals(retrieved.get(), filter);
    }

    @Test
    public void testGetNonexistent()
    {
        QueryId queryId = QueryId.valueOf("nonexistent_query");
        String filterId = "nonexistent_filter";

        Optional<JoinDynamicFilter> retrieved = service.getFilter(queryId, filterId);
        assertFalse(retrieved.isPresent());
    }

    @Test
    public void testGetNonexistentFilter()
    {
        QueryId queryId = QueryId.valueOf("test_query_1");
        JoinDynamicFilter filter = createTestFilter();

        service.registerFilter(queryId, "filter_1", filter);

        // Query exists but filter doesn't
        Optional<JoinDynamicFilter> retrieved = service.getFilter(queryId, "nonexistent_filter");
        assertFalse(retrieved.isPresent());
    }

    @Test
    public void testHasFilter()
    {
        QueryId queryId = QueryId.valueOf("test_query_1");
        String filterId = "filter_1";
        JoinDynamicFilter filter = createTestFilter();

        // Before registration
        assertFalse(service.hasFilter(queryId, filterId));

        // After registration
        service.registerFilter(queryId, filterId, filter);
        assertTrue(service.hasFilter(queryId, filterId));

        // Different filter ID
        assertFalse(service.hasFilter(queryId, "other_filter"));
    }

    @Test
    public void testRemoveFiltersForQuery()
    {
        QueryId queryId = QueryId.valueOf("test_query_1");
        JoinDynamicFilter filter1 = createTestFilter();
        JoinDynamicFilter filter2 = createTestFilter();

        service.registerFilter(queryId, "filter_1", filter1);
        service.registerFilter(queryId, "filter_2", filter2);

        assertTrue(service.hasFilter(queryId, "filter_1"));
        assertTrue(service.hasFilter(queryId, "filter_2"));

        // Remove all filters for query
        service.removeFiltersForQuery(queryId);

        assertFalse(service.hasFilter(queryId, "filter_1"));
        assertFalse(service.hasFilter(queryId, "filter_2"));
    }

    @Test
    public void testMultipleQueries()
    {
        QueryId queryId1 = QueryId.valueOf("test_query_1");
        QueryId queryId2 = QueryId.valueOf("test_query_2");
        JoinDynamicFilter filter1 = createTestFilter();
        JoinDynamicFilter filter2 = createTestFilter();

        service.registerFilter(queryId1, "filter_1", filter1);
        service.registerFilter(queryId2, "filter_1", filter2);

        // Verify isolation - same filter ID, different queries
        Optional<JoinDynamicFilter> retrieved1 = service.getFilter(queryId1, "filter_1");
        Optional<JoinDynamicFilter> retrieved2 = service.getFilter(queryId2, "filter_1");

        assertTrue(retrieved1.isPresent());
        assertTrue(retrieved2.isPresent());
        assertEquals(retrieved1.get(), filter1);
        assertEquals(retrieved2.get(), filter2);

        // Removing query1 doesn't affect query2
        service.removeFiltersForQuery(queryId1);
        assertFalse(service.hasFilter(queryId1, "filter_1"));
        assertTrue(service.hasFilter(queryId2, "filter_1"));
    }

    @Test
    public void testMultipleFiltersPerQuery()
    {
        QueryId queryId = QueryId.valueOf("test_query_1");
        JoinDynamicFilter filter1 = createTestFilter();
        JoinDynamicFilter filter2 = createTestFilter();
        JoinDynamicFilter filter3 = createTestFilter();

        service.registerFilter(queryId, "filter_1", filter1);
        service.registerFilter(queryId, "filter_2", filter2);
        service.registerFilter(queryId, "filter_3", filter3);

        // All filters accessible
        assertTrue(service.hasFilter(queryId, "filter_1"));
        assertTrue(service.hasFilter(queryId, "filter_2"));
        assertTrue(service.hasFilter(queryId, "filter_3"));

        assertEquals(service.getFilter(queryId, "filter_1").get(), filter1);
        assertEquals(service.getFilter(queryId, "filter_2").get(), filter2);
        assertEquals(service.getFilter(queryId, "filter_3").get(), filter3);
    }

    @Test
    public void testGetAllFiltersForQuery()
    {
        QueryId queryId = QueryId.valueOf("test_query_1");
        JoinDynamicFilter filter1 = createTestFilter();
        JoinDynamicFilter filter2 = createTestFilter();

        service.registerFilter(queryId, "filter_1", filter1);
        service.registerFilter(queryId, "filter_2", filter2);

        Map<String, JoinDynamicFilter> allFilters = service.getAllFiltersForQuery(queryId);
        assertEquals(allFilters.size(), 2);
        assertEquals(allFilters.get("filter_1"), filter1);
        assertEquals(allFilters.get("filter_2"), filter2);
    }

    @Test
    public void testGetAllFiltersForNonexistentQuery()
    {
        QueryId queryId = QueryId.valueOf("nonexistent_query");

        Map<String, JoinDynamicFilter> allFilters = service.getAllFiltersForQuery(queryId);
        assertTrue(allFilters.isEmpty());
    }

    @Test
    public void testFilterIdPreserved()
    {
        QueryId queryId = QueryId.valueOf("test_query_filter_id");
        String filterId = "549";
        JoinDynamicFilter filter = createTestFilterWithId(filterId);

        service.registerFilter(queryId, filterId, filter);

        Optional<JoinDynamicFilter> retrieved = service.getFilter(queryId, filterId);
        assertTrue(retrieved.isPresent());
        assertEquals(retrieved.get().getFilterId(), filterId);
    }

    @Test
    public void testScanFilterMapping()
    {
        QueryId queryId = QueryId.valueOf("test_query_scan_mapping");
        PlanNodeId scanNodeId = new PlanNodeId("scan_1");
        Set<String> filterIds = ImmutableSet.of("filter_1", "filter_2");

        assertTrue(service.getFilterIdsForScan(queryId, scanNodeId).isEmpty());

        service.registerScanFilterMapping(queryId, scanNodeId, filterIds);

        Set<String> retrieved = service.getFilterIdsForScan(queryId, scanNodeId);
        assertEquals(retrieved, filterIds);
    }

    @Test
    public void testScanFilterMappingMultipleScans()
    {
        QueryId queryId = QueryId.valueOf("test_query_multi_scan");
        PlanNodeId scan1 = new PlanNodeId("scan_1");
        PlanNodeId scan2 = new PlanNodeId("scan_2");

        service.registerScanFilterMapping(queryId, scan1, ImmutableSet.of("filter_1"));
        service.registerScanFilterMapping(queryId, scan2, ImmutableSet.of("filter_2", "filter_3"));

        assertEquals(service.getFilterIdsForScan(queryId, scan1), ImmutableSet.of("filter_1"));
        assertEquals(service.getFilterIdsForScan(queryId, scan2), ImmutableSet.of("filter_2", "filter_3"));
    }

    @Test
    public void testScanFilterMappingNonexistent()
    {
        QueryId queryId = QueryId.valueOf("nonexistent_query");
        PlanNodeId scanNodeId = new PlanNodeId("scan_1");

        assertTrue(service.getFilterIdsForScan(queryId, scanNodeId).isEmpty());
    }

    @Test
    public void testRemoveFiltersAlsoClearsScanMappings()
    {
        QueryId queryId = QueryId.valueOf("test_query_cleanup");
        PlanNodeId scanNodeId = new PlanNodeId("scan_1");

        service.registerFilter(queryId, "filter_1", createTestFilter());
        service.registerScanFilterMapping(queryId, scanNodeId, ImmutableSet.of("filter_1"));

        assertTrue(service.hasFilter(queryId, "filter_1"));
        assertEquals(service.getFilterIdsForScan(queryId, scanNodeId), ImmutableSet.of("filter_1"));

        service.removeFiltersForQuery(queryId);
        assertFalse(service.hasFilter(queryId, "filter_1"));
        assertTrue(service.getFilterIdsForScan(queryId, scanNodeId).isEmpty());
    }

    @Test
    public void testScanFilterMappingIsolationBetweenQueries()
    {
        QueryId query1 = QueryId.valueOf("test_query_1");
        QueryId query2 = QueryId.valueOf("test_query_2");
        PlanNodeId scanNodeId = new PlanNodeId("scan_1");

        service.registerScanFilterMapping(query1, scanNodeId, ImmutableSet.of("filter_1"));
        service.registerScanFilterMapping(query2, scanNodeId, ImmutableSet.of("filter_2"));

        assertEquals(service.getFilterIdsForScan(query1, scanNodeId), ImmutableSet.of("filter_1"));
        assertEquals(service.getFilterIdsForScan(query2, scanNodeId), ImmutableSet.of("filter_2"));

        // Removing query1 doesn't affect query2
        service.removeFiltersForQuery(query1);
        assertTrue(service.getFilterIdsForScan(query1, scanNodeId).isEmpty());
        assertEquals(service.getFilterIdsForScan(query2, scanNodeId), ImmutableSet.of("filter_2"));
    }

    @Test
    public void testSetExpectedPartitionsForRemoteSourceBuildSide()
    {
        // Regression test: setExpectedPartitionsForFilters must set expected partitions
        // for JoinNodes whose build side (right child) is a RemoteSourceNode.
        // Previously, such joins were skipped, leaving expectedPartitions at MAX_VALUE
        // so the filter could never complete.
        QueryId queryId = QueryId.valueOf("test_remote_build");
        String filterId = "514";
        JoinDynamicFilter filter = createTestFilterWithId(filterId);
        service.registerFilter(queryId, filterId, filter);

        // Build a JoinNode with RemoteSourceNode as right child (partitioned join)
        VariableReferenceExpression probeVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BIGINT);
        VariableReferenceExpression buildVar = new VariableReferenceExpression(Optional.empty(), "customer_id_0", BIGINT);

        RemoteSourceNode probeSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("probe_source"), new PlanFragmentId(3),
                ImmutableList.of(probeVar), false, Optional.empty(), REPARTITION);
        RemoteSourceNode buildSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("build_source"), new PlanFragmentId(4),
                ImmutableList.of(buildVar), false, Optional.empty(), REPARTITION);

        JoinNode joinNode = new JoinNode(
                Optional.empty(),
                new PlanNodeId("join_1"),
                INNER,
                probeSource,
                buildSource,
                ImmutableList.of(new EquiJoinClause(probeVar, buildVar)),
                ImmutableList.of(probeVar, buildVar),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(filterId, buildVar));

        // Before: filter cannot complete (expectedPartitions = MAX_VALUE)
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of(filterId, com.facebook.presto.common.predicate.Domain.singleValue(BIGINT, 1L))));
        assertFalse(filter.isComplete(), "Filter should not complete before setExpectedPartitions is called");

        // Act: setExpectedPartitionsForFilters should process the JoinNode
        // even though its right child is a RemoteSourceNode
        SectionExecutionFactory.setExpectedPartitionsForFilters(service, queryId, joinNode, 1);

        // After: filter should now be complete (1 partition received >= 1 expected)
        assertTrue(filter.isComplete(),
                "Filter should complete after setExpectedPartitionsForFilters processes RemoteSourceNode build side");
    }

    @Test
    public void testCrossFragmentFilterMatchingSimplePartitionedJoin()
    {
        // Tests that in a simple partitioned join (JoinNode with RemoteSourceNode
        // children), the probe-side child fragment's scan IS wired to the filter
        // (for split pruning), but the build-side child fragment's scan is NOT.

        String filterId = "100";
        VariableReferenceExpression probeVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BIGINT);
        VariableReferenceExpression buildVar = new VariableReferenceExpression(Optional.empty(), "customer_id_0", BIGINT);

        // Parent fragment (fragment 1): JoinNode with two RemoteSourceNode children
        RemoteSourceNode probeSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("probe_source"), new PlanFragmentId(2),
                ImmutableList.of(probeVar), false, Optional.empty(), REPARTITION);
        RemoteSourceNode buildSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("build_source"), new PlanFragmentId(3),
                ImmutableList.of(buildVar), false, Optional.empty(), REPARTITION);

        JoinNode joinNode = new JoinNode(
                Optional.empty(),
                new PlanNodeId("join_1"),
                INNER,
                probeSource,
                buildSource,
                ImmutableList.of(new EquiJoinClause(probeVar, buildVar)),
                ImmutableList.of(probeVar, buildVar),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(filterId, buildVar));

        PlanFragment parentFragment = new PlanFragment(
                new PlanFragmentId(1),
                joinNode,
                ImmutableSet.of(probeVar, buildVar),
                SINGLE_DISTRIBUTION,
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(probeVar, buildVar)),
                Optional.empty(),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.of(StatsAndCosts.empty()),
                Optional.empty());

        // Probe-side child fragment (fragment 2): TableScanNode with output "customer_id"
        PlanNodeId probeScanId = new PlanNodeId("probe_scan");
        VariableReferenceExpression probeChildVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BIGINT);
        PlanFragment probeChildFragment = createScanFragment(2, probeScanId, probeChildVar, "customer_id");

        // Build-side child fragment (fragment 3): TableScanNode with output "customer_id"
        PlanNodeId buildScanId = new PlanNodeId("build_scan");
        VariableReferenceExpression buildChildVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BIGINT);
        PlanFragment buildChildFragment = createScanFragment(3, buildScanId, buildChildVar, "customer_id");

        Session session = createDppSession();
        QueryId queryId = session.getQueryId();

        SplitSourceFactory factory = createSplitSourceFactory();

        // Register parent fragment first (mimics root-first traversal)
        factory.registerDynamicFilters(parentFragment, session);
        assertTrue(service.hasFilter(queryId, filterId));

        factory.registerDynamicFilters(probeChildFragment, session);
        factory.registerDynamicFilters(buildChildFragment, session);

        // Probe-side scan SHOULD be wired (split pruning, no circular dependency)
        assertEquals(service.getFilterIdsForScan(queryId, probeScanId), ImmutableSet.of(filterId),
                "Probe-side child fragment's scan should be wired to parent's filter for split pruning.");

        // Build-side scan should NOT be wired (would create circular dependency)
        assertTrue(service.getFilterIdsForScan(queryId, buildScanId).isEmpty(),
                "Build-side child fragment's scan must not be wired to parent's filter.");
    }

    @Test
    public void testDeepDescendantScansNotWiredToOuterJoinFilter()
    {
        // Regression test for TPC-DS Q4: Fragment 1 has nested JoinNodes where
        // the outer join's probe child is another JoinNode (not a RemoteSourceNode).
        // Build-side descendant fragments must NOT receive filters from outer joins.
        //
        // Fragment 1 structure:
        //   JoinNode A (filter "200" on customer_id)
        //     probe: JoinNode B (filter "300" on customer_id)
        //       probe: RemoteSource(frag 2) — fact table
        //       build: RemoteSource(frag 3) — customer table
        //     build: RemoteSource(frag 4) — dimension table
        //
        // The fix follows the "probe chain" through nested JoinNodes: at each
        // JoinNode boundary, only the probe (left) child is traversed, never
        // the build (right) child.
        //
        // Expected behavior:
        //   - Fragment 2 gets BOTH filters "200" and "300": JoinNode B registers
        //     "300" for frag 2 (direct probe child), and JoinNode A's probe chain
        //     follows through JoinNode B's probe to reach frag 2 (registers "200")
        //   - Fragment 3 gets NO filter (build side of JoinNode B — skipped)
        //   - Fragment 4 gets NO filter (build side of JoinNode A — skipped)

        String outerFilterId = "200";
        String innerFilterId = "300";

        VariableReferenceExpression customerIdVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BIGINT);
        VariableReferenceExpression customerIdBuildVar = new VariableReferenceExpression(Optional.empty(), "customer_id_0", BIGINT);
        VariableReferenceExpression dimVar = new VariableReferenceExpression(Optional.empty(), "customer_id_1", BIGINT);

        // Inner JoinNode B: probe=RemoteSource(frag 2), build=RemoteSource(frag 3)
        RemoteSourceNode innerProbeSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("inner_probe_source"), new PlanFragmentId(2),
                ImmutableList.of(customerIdVar), false, Optional.empty(), REPARTITION);
        RemoteSourceNode innerBuildSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("inner_build_source"), new PlanFragmentId(3),
                ImmutableList.of(customerIdBuildVar), false, Optional.empty(), REPARTITION);

        JoinNode innerJoin = new JoinNode(
                Optional.empty(),
                new PlanNodeId("join_B"),
                INNER,
                innerProbeSource,
                innerBuildSource,
                ImmutableList.of(new EquiJoinClause(customerIdVar, customerIdBuildVar)),
                ImmutableList.of(customerIdVar, customerIdBuildVar),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(innerFilterId, customerIdBuildVar));

        // Outer JoinNode A: probe=JoinNode B, build=RemoteSource(frag 4)
        RemoteSourceNode outerBuildSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("outer_build_source"), new PlanFragmentId(4),
                ImmutableList.of(dimVar), false, Optional.empty(), REPARTITION);

        JoinNode outerJoin = new JoinNode(
                Optional.empty(),
                new PlanNodeId("join_A"),
                INNER,
                innerJoin,
                outerBuildSource,
                ImmutableList.of(new EquiJoinClause(customerIdVar, dimVar)),
                ImmutableList.of(customerIdVar, dimVar),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(outerFilterId, dimVar));

        PlanFragment parentFragment = new PlanFragment(
                new PlanFragmentId(1),
                outerJoin,
                ImmutableSet.of(customerIdVar, dimVar),
                SINGLE_DISTRIBUTION,
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(customerIdVar, dimVar)),
                Optional.empty(),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.of(StatsAndCosts.empty()),
                Optional.empty());

        // Fragment 2: fact table scan with "customer_id" output
        PlanNodeId factScanId = new PlanNodeId("fact_scan");
        VariableReferenceExpression factVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BIGINT);
        PlanFragment factFragment = createScanFragment(2, factScanId, factVar, "customer_id");

        // Fragment 3: customer table scan with "customer_id" output (build side of inner join)
        PlanNodeId customerScanId = new PlanNodeId("customer_scan");
        VariableReferenceExpression custVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BIGINT);
        PlanFragment customerFragment = createScanFragment(3, customerScanId, custVar, "customer_id");

        // Fragment 4: dimension table scan with "customer_id" output (build side of outer join)
        PlanNodeId dimScanId = new PlanNodeId("dim_scan");
        VariableReferenceExpression dimScanVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BIGINT);
        PlanFragment dimFragment = createScanFragment(4, dimScanId, dimScanVar, "customer_id");

        Session session = createDppSession();
        QueryId queryId = session.getQueryId();

        SplitSourceFactory factory = createSplitSourceFactory();

        // Register parent fragment first (root-first traversal)
        factory.registerDynamicFilters(parentFragment, session);
        assertTrue(service.hasFilter(queryId, outerFilterId));
        assertTrue(service.hasFilter(queryId, innerFilterId));

        // Register child fragments
        factory.registerDynamicFilters(factFragment, session);
        factory.registerDynamicFilters(customerFragment, session);
        factory.registerDynamicFilters(dimFragment, session);

        // Fragment 2 (fact scan): should get BOTH filters
        // - "300" from JoinNode B (direct probe child)
        // - "200" from JoinNode A (probe chain through JoinNode B's probe)
        assertEquals(service.getFilterIdsForScan(queryId, factScanId), ImmutableSet.of(innerFilterId, outerFilterId),
                "Fact scan should receive both outer and inner filters via probe chain.");

        // Fragment 3 (customer scan): should NOT be wired to any filter
        // (build side of inner JoinNode B — no probe registration)
        assertTrue(service.getFilterIdsForScan(queryId, customerScanId).isEmpty(),
                "Customer scan (build side of inner join) must not be wired to any filter.");

        // Fragment 4 (dimension scan): should NOT be wired to any filter
        // (build side of outer JoinNode A — no probe registration)
        assertTrue(service.getFilterIdsForScan(queryId, dimScanId).isEmpty(),
                "Dimension scan (build side of outer join) must not be wired to any filter.");
    }

    private static Session createDppSession()
    {
        return testSessionBuilder()
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "ALWAYS")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME, "2s")
                .build();
    }

    private SplitSourceFactory createSplitSourceFactory()
    {
        return new SplitSourceFactory(
                new SplitSourceProvider() {
                    @Override
                    public SplitSource getSplits(Session s, TableHandle t, SplitSchedulingStrategy st, WarningCollector w)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SplitSource getSplits(Session s, TableFunctionHandle t)
                    {
                        throw new UnsupportedOperationException();
                    }
                },
                WarningCollector.NOOP,
                service);
    }

    private static PlanFragment createScanFragment(int fragmentId, PlanNodeId scanId, VariableReferenceExpression variable, String columnName)
    {
        TableScanNode scan = createTableScan(scanId, variable, columnName);
        return new PlanFragment(
                new PlanFragmentId(fragmentId),
                scan,
                ImmutableSet.of(variable),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(scanId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(variable)),
                Optional.empty(),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.of(StatsAndCosts.empty()),
                Optional.empty());
    }

    private static TableScanNode createTableScan(PlanNodeId scanId, VariableReferenceExpression variable, String columnName)
    {
        return new TableScanNode(
                Optional.empty(),
                scanId,
                new TableHandle(
                        new ConnectorId("test"),
                        new TestingTableHandle(),
                        TestingTransactionHandle.create(),
                        Optional.empty()),
                ImmutableList.of(variable),
                ImmutableMap.of(variable, new TestingColumnHandle(columnName)),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
    }

    private JoinDynamicFilter createTestFilter()
    {
        return new JoinDynamicFilter(DEFAULT_TIMEOUT);
    }

    private JoinDynamicFilter createTestFilterWithId(String filterId)
    {
        return new JoinDynamicFilter(filterId, "column_a", DEFAULT_TIMEOUT, new DynamicFilterStats(), new RuntimeStats());
    }
}
