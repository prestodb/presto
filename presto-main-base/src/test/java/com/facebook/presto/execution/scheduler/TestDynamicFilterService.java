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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.split.SplitSourceProvider;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_STRATEGY;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
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

        java.util.Optional<JoinDynamicFilter> retrieved = service.getFilter(queryId, filterId);
        assertTrue(retrieved.isPresent());
        assertEquals(retrieved.get(), filter);
    }

    @Test
    public void testGetNonexistent()
    {
        QueryId queryId = QueryId.valueOf("nonexistent_query");
        String filterId = "nonexistent_filter";

        java.util.Optional<JoinDynamicFilter> retrieved = service.getFilter(queryId, filterId);
        assertFalse(retrieved.isPresent());
    }

    @Test
    public void testGetNonexistentFilter()
    {
        QueryId queryId = QueryId.valueOf("test_query_1");
        JoinDynamicFilter filter = createTestFilter();

        service.registerFilter(queryId, "filter_1", filter);

        java.util.Optional<JoinDynamicFilter> retrieved = service.getFilter(queryId, "nonexistent_filter");
        assertFalse(retrieved.isPresent());
    }

    @Test
    public void testHasFilter()
    {
        QueryId queryId = QueryId.valueOf("test_query_1");
        String filterId = "filter_1";
        JoinDynamicFilter filter = createTestFilter();

        assertFalse(service.hasFilter(queryId, filterId));

        service.registerFilter(queryId, filterId, filter);
        assertTrue(service.hasFilter(queryId, filterId));

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

        java.util.Optional<JoinDynamicFilter> retrieved1 = service.getFilter(queryId1, "filter_1");
        java.util.Optional<JoinDynamicFilter> retrieved2 = service.getFilter(queryId2, "filter_1");

        assertTrue(retrieved1.isPresent());
        assertTrue(retrieved2.isPresent());
        assertEquals(retrieved1.get(), filter1);
        assertEquals(retrieved2.get(), filter2);

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

        java.util.Optional<JoinDynamicFilter> retrieved = service.getFilter(queryId, filterId);
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
    public void testScanFilterMappingMergesOnDuplicateRegistration()
    {
        // Regression test: registerScanFilterMapping must merge filter IDs
        // when called multiple times for the same scan (e.g., local filters
        // from Step 1 and cross-fragment filters from Step 2).
        QueryId queryId = QueryId.valueOf("test_query_merge");
        PlanNodeId scanNodeId = new PlanNodeId("scan_1");

        service.registerScanFilterMapping(queryId, scanNodeId, ImmutableSet.of("filter_date"));
        service.registerScanFilterMapping(queryId, scanNodeId, ImmutableSet.of("filter_customer"));

        assertEquals(service.getFilterIdsForScan(queryId, scanNodeId),
                ImmutableSet.of("filter_date", "filter_customer"));
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

        service.removeFiltersForQuery(query1);
        assertTrue(service.getFilterIdsForScan(query1, scanNodeId).isEmpty());
        assertEquals(service.getFilterIdsForScan(query2, scanNodeId), ImmutableSet.of("filter_2"));
    }

    @Test
    public void testSetExpectedPartitionsForRemoteSourceBuildSide()
    {
        QueryId queryId = QueryId.valueOf("test_remote_build");
        String filterId = "514";
        JoinDynamicFilter filter = createTestFilterWithId(filterId);
        service.registerFilter(queryId, filterId, filter);

        VariableReferenceExpression probeVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BigintType.BIGINT);
        VariableReferenceExpression buildVar = new VariableReferenceExpression(Optional.empty(), "customer_id_0", BigintType.BIGINT);
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

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of(filterId, com.facebook.presto.common.predicate.Domain.singleValue(BigintType.BIGINT, 1L))));
        assertFalse(filter.isComplete(), "filter should not be complete before setExpectedPartitions");

        SectionExecutionFactory.setExpectedPartitionsForFilters(service, queryId, joinNode, 1);

        assertTrue(filter.isComplete(), "filter should complete after setExpectedPartitionsForFilters");
    }

    @Test
    public void testForEachJoinDynamicFilterFlagsReplicateBuildAsBroadcast()
    {
        QueryId queryId = QueryId.valueOf("test_replicate_build");
        String filterId = "f";
        JoinDynamicFilter filter = createTestFilterWithId(filterId);
        service.registerFilter(queryId, filterId, filter);

        java.util.Map<String, Boolean> seen = new java.util.HashMap<>();
        SectionExecutionFactory.forEachJoinDynamicFilter(
                service, queryId, joinNodeWithRemoteBuild(filterId, REPLICATE),
                (f, isBroadcastBuild) -> seen.put(f.getFilterId(), isBroadcastBuild));
        assertEquals(seen.get(filterId), Boolean.TRUE);
    }

    @Test
    public void testForEachJoinDynamicFilterFlagsRepartitionBuildAsNonBroadcast()
    {
        QueryId queryId = QueryId.valueOf("test_repartition_build");
        String filterId = "f";
        JoinDynamicFilter filter = createTestFilterWithId(filterId);
        service.registerFilter(queryId, filterId, filter);

        java.util.Map<String, Boolean> seen = new java.util.HashMap<>();
        SectionExecutionFactory.forEachJoinDynamicFilter(
                service, queryId, joinNodeWithRemoteBuild(filterId, REPARTITION),
                (f, isBroadcastBuild) -> seen.put(f.getFilterId(), isBroadcastBuild));
        assertEquals(seen.get(filterId), Boolean.FALSE);
    }

    @Test
    public void testForEachJoinDynamicFilterFlagsLocalBuildAsNonBroadcast()
    {
        QueryId queryId = QueryId.valueOf("test_local_build");
        String filterId = "f";
        JoinDynamicFilter filter = createTestFilterWithId(filterId);
        service.registerFilter(queryId, filterId, filter);

        java.util.Map<String, Boolean> seen = new java.util.HashMap<>();
        SectionExecutionFactory.forEachJoinDynamicFilter(
                service, queryId, joinNodeWithLocalBuild(filterId),
                (f, isBroadcastBuild) -> seen.put(f.getFilterId(), isBroadcastBuild));
        assertEquals(seen.get(filterId), Boolean.FALSE);
    }

    @Test
    public void testCrossFragmentFilterMatchingSimplePartitionedJoin()
    {
        String filterId = "100";
        VariableReferenceExpression probeVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BigintType.BIGINT);
        VariableReferenceExpression buildVar = new VariableReferenceExpression(Optional.empty(), "customer_id_0", BigintType.BIGINT);

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

        PlanNodeId probeScanId = new PlanNodeId("probe_scan");
        VariableReferenceExpression probeChildVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BigintType.BIGINT);
        PlanFragment probeChildFragment = createScanFragment(2, probeScanId, probeChildVar, "customer_id");

        PlanNodeId buildScanId = new PlanNodeId("build_scan");
        VariableReferenceExpression buildChildVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BigintType.BIGINT);
        PlanFragment buildChildFragment = createScanFragment(3, buildScanId, buildChildVar, "customer_id");

        Session session = createDppSession();
        QueryId queryId = session.getQueryId();
        SplitSourceFactory factory = createSplitSourceFactory();

        StreamingSubPlan probeChild = new StreamingSubPlan(probeChildFragment, ImmutableList.of());
        StreamingSubPlan buildChild = new StreamingSubPlan(buildChildFragment, ImmutableList.of());
        StreamingSubPlan root = new StreamingSubPlan(parentFragment, ImmutableList.of(probeChild, buildChild));
        factory.registerDynamicFilters(root, session);

        assertTrue(service.hasFilter(queryId, filterId));
        assertEquals(service.getFilterIdsForScan(queryId, probeScanId), ImmutableSet.of(filterId),
                "Probe-side child fragment scan should be wired to filter");
        assertTrue(service.getFilterIdsForScan(queryId, buildScanId).isEmpty(),
                "Build-side child fragment scan must not be wired");
    }

    @Test
    public void testDeepDescendantScansNotWiredToOuterJoinFilter()
    {
        String outerFilterId = "200";
        String innerFilterId = "300";
        VariableReferenceExpression customerIdVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BigintType.BIGINT);
        VariableReferenceExpression customerIdBuildVar = new VariableReferenceExpression(Optional.empty(), "customer_id_0", BigintType.BIGINT);
        VariableReferenceExpression dimVar = new VariableReferenceExpression(Optional.empty(), "customer_id_1", BigintType.BIGINT);

        RemoteSourceNode innerProbeSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("inner_probe_source"), new PlanFragmentId(2),
                ImmutableList.of(customerIdVar), false, Optional.empty(), REPARTITION);
        RemoteSourceNode innerBuildSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("inner_build_source"), new PlanFragmentId(3),
                ImmutableList.of(customerIdBuildVar), false, Optional.empty(), REPARTITION);
        JoinNode innerJoin = new JoinNode(
                Optional.empty(), new PlanNodeId("join_B"), INNER,
                innerProbeSource, innerBuildSource,
                ImmutableList.of(new EquiJoinClause(customerIdVar, customerIdBuildVar)),
                ImmutableList.of(customerIdVar, customerIdBuildVar),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                ImmutableMap.of(innerFilterId, customerIdBuildVar));

        RemoteSourceNode outerBuildSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("outer_build_source"), new PlanFragmentId(4),
                ImmutableList.of(dimVar), false, Optional.empty(), REPARTITION);
        JoinNode outerJoin = new JoinNode(
                Optional.empty(), new PlanNodeId("join_A"), INNER,
                innerJoin, outerBuildSource,
                ImmutableList.of(new EquiJoinClause(customerIdVar, dimVar)),
                ImmutableList.of(customerIdVar, dimVar),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                ImmutableMap.of(outerFilterId, dimVar));

        PlanFragment parentFragment = new PlanFragment(
                new PlanFragmentId(1), outerJoin,
                ImmutableSet.of(customerIdVar, dimVar), SINGLE_DISTRIBUTION, ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(customerIdVar, dimVar)),
                Optional.empty(), StageExecutionDescriptor.ungroupedExecution(), false,
                Optional.of(StatsAndCosts.empty()), Optional.empty());

        PlanNodeId factScanId = new PlanNodeId("fact_scan");
        PlanFragment factFragment = createScanFragment(2, factScanId,
                new VariableReferenceExpression(Optional.empty(), "customer_id", BigintType.BIGINT), "customer_id");

        PlanNodeId customerScanId = new PlanNodeId("customer_scan");
        PlanFragment customerFragment = createScanFragment(3, customerScanId,
                new VariableReferenceExpression(Optional.empty(), "customer_id", BigintType.BIGINT), "customer_id");

        PlanNodeId dimScanId = new PlanNodeId("dim_scan");
        PlanFragment dimFragment = createScanFragment(4, dimScanId,
                new VariableReferenceExpression(Optional.empty(), "customer_id", BigintType.BIGINT), "customer_id");

        Session session = createDppSession();
        QueryId queryId = session.getQueryId();
        SplitSourceFactory factory = createSplitSourceFactory();

        StreamingSubPlan factChild = new StreamingSubPlan(factFragment, ImmutableList.of());
        StreamingSubPlan customerChild = new StreamingSubPlan(customerFragment, ImmutableList.of());
        StreamingSubPlan dimChild = new StreamingSubPlan(dimFragment, ImmutableList.of());
        StreamingSubPlan root = new StreamingSubPlan(parentFragment, ImmutableList.of(factChild, customerChild, dimChild));
        factory.registerDynamicFilters(root, session);

        assertTrue(service.hasFilter(queryId, outerFilterId));
        assertTrue(service.hasFilter(queryId, innerFilterId));

        assertEquals(service.getFilterIdsForScan(queryId, factScanId), ImmutableSet.of(innerFilterId, outerFilterId),
                "Fact scan should receive both filters via probe chain");
        assertTrue(service.getFilterIdsForScan(queryId, customerScanId).isEmpty(),
                "Customer scan (build side of inner join) must not be wired");
        assertTrue(service.getFilterIdsForScan(queryId, dimScanId).isEmpty(),
                "Dim scan (build side of outer join) must not be wired");
    }

    @Test
    public void testSameFragmentFilterMatchesWhenRootProjectStripsProbeColumn()
    {
        String filterId = "500";
        VariableReferenceExpression orderIdVar = new VariableReferenceExpression(Optional.empty(), "order_id", BigintType.BIGINT);
        VariableReferenceExpression customerIdVar = new VariableReferenceExpression(Optional.empty(), "customer_id", BigintType.BIGINT);
        VariableReferenceExpression buildVar = new VariableReferenceExpression(Optional.empty(), "customer_id_0", BigintType.BIGINT);

        PlanNodeId factScanId = new PlanNodeId("fact_scan");
        TableScanNode factScan = createTableScan(factScanId,
                ImmutableList.of(orderIdVar, customerIdVar),
                ImmutableMap.of(orderIdVar, "order_id", customerIdVar, "customer_id"));

        RemoteSourceNode buildSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("build_source"), new PlanFragmentId(2),
                ImmutableList.of(buildVar), false, Optional.empty(), REPARTITION);
        JoinNode joinNode = new JoinNode(
                Optional.empty(), new PlanNodeId("join_1"), INNER,
                factScan, buildSource,
                ImmutableList.of(new EquiJoinClause(customerIdVar, buildVar)),
                ImmutableList.of(orderIdVar, customerIdVar, buildVar),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                ImmutableMap.of(filterId, buildVar));

        ProjectNode rootProject = new ProjectNode(
                new PlanNodeId("project_root"),
                joinNode,
                Assignments.builder().put(orderIdVar, orderIdVar).build());

        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId(1), rootProject,
                ImmutableSet.of(orderIdVar), SOURCE_DISTRIBUTION, ImmutableList.of(factScanId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(orderIdVar)),
                Optional.empty(), StageExecutionDescriptor.ungroupedExecution(), false,
                Optional.of(StatsAndCosts.empty()), Optional.empty());

        Session session = createDppSession();
        QueryId queryId = session.getQueryId();
        SplitSourceFactory factory = createSplitSourceFactory();

        factory.registerDynamicFilters(new StreamingSubPlan(fragment, ImmutableList.of()), session);

        assertTrue(service.hasFilter(queryId, filterId));
        assertEquals(service.getFilterIdsForScan(queryId, factScanId), ImmutableSet.of(filterId),
                "Same-fragment filter should match probe-side scan even when root project strips probe column");
    }

    private JoinNode joinNodeWithRemoteBuild(String filterId, ExchangeNode.Type buildExchangeType)
    {
        VariableReferenceExpression probeVar = new VariableReferenceExpression(Optional.empty(), "probe", BigintType.BIGINT);
        VariableReferenceExpression buildVar = new VariableReferenceExpression(Optional.empty(), "build", BigintType.BIGINT);
        RemoteSourceNode probeSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("probe_src"), new PlanFragmentId(1),
                ImmutableList.of(probeVar), false, Optional.empty(), REPARTITION);
        RemoteSourceNode buildSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("build_src"), new PlanFragmentId(2),
                ImmutableList.of(buildVar), false, Optional.empty(), buildExchangeType);
        return joinNode(filterId, probeSource, buildSource, probeVar, buildVar);
    }

    private JoinNode joinNodeWithLocalBuild(String filterId)
    {
        VariableReferenceExpression probeVar = new VariableReferenceExpression(Optional.empty(), "probe", BigintType.BIGINT);
        VariableReferenceExpression buildVar = new VariableReferenceExpression(Optional.empty(), "build", BigintType.BIGINT);
        RemoteSourceNode probeSource = new RemoteSourceNode(
                Optional.empty(), new PlanNodeId("probe_src"), new PlanFragmentId(1),
                ImmutableList.of(probeVar), false, Optional.empty(), REPARTITION);
        ValuesNode localBuild = new ValuesNode(
                Optional.empty(), new PlanNodeId("local_build"),
                ImmutableList.of(buildVar), ImmutableList.of(), Optional.empty());
        return joinNode(filterId, probeSource, localBuild, probeVar, buildVar);
    }

    private JoinNode joinNode(String filterId, com.facebook.presto.spi.plan.PlanNode probe,
            com.facebook.presto.spi.plan.PlanNode build,
            VariableReferenceExpression probeVar, VariableReferenceExpression buildVar)
    {
        return new JoinNode(
                Optional.empty(), new PlanNodeId("join"), INNER,
                probe, build,
                ImmutableList.of(new EquiJoinClause(probeVar, buildVar)),
                ImmutableList.of(probeVar, buildVar),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                ImmutableMap.of(filterId, buildVar));
    }

    private static Session createDppSession()
    {
        return testSessionBuilder()
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "ALWAYS")
                .setSystemProperty("distributed_dynamic_filter_max_wait_time", "2s")
                .build();
    }

    private SplitSourceFactory createSplitSourceFactory()
    {
        return new SplitSourceFactory(
                new SplitSourceProvider()
                {
                    @Override
                    public SplitSource getSplits(Session s, com.facebook.presto.spi.TableHandle t,
                            SplitSchedulingStrategy st, WarningCollector w)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SplitSource getSplits(Session s, com.facebook.presto.metadata.TableFunctionHandle t)
                    {
                        throw new UnsupportedOperationException();
                    }
                },
                WarningCollector.NOOP,
                service,
                MetadataManager.createTestMetadataManager());
    }

    private static PlanFragment createScanFragment(int fragmentId, PlanNodeId scanId,
            VariableReferenceExpression variable, String columnName)
    {
        TableScanNode scan = createTableScan(scanId, variable, columnName);
        return new PlanFragment(
                new PlanFragmentId(fragmentId), scan,
                ImmutableSet.of(variable), SOURCE_DISTRIBUTION, ImmutableList.of(scanId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(variable)),
                Optional.empty(), StageExecutionDescriptor.ungroupedExecution(), false,
                Optional.of(StatsAndCosts.empty()), Optional.empty());
    }

    private static TableScanNode createTableScan(PlanNodeId scanId, VariableReferenceExpression variable,
            String columnName)
    {
        return new TableScanNode(
                Optional.empty(), scanId,
                new TableHandle(new ConnectorId("test"), new TestingTableHandle(),
                        TestingTransactionHandle.create(), Optional.empty()),
                ImmutableList.of(variable),
                ImmutableMap.of(variable, new TestingColumnHandle(columnName)),
                TupleDomain.all(), TupleDomain.all(), Optional.empty());
    }

    private static TableScanNode createTableScan(PlanNodeId scanId, List<VariableReferenceExpression> variables,
            Map<VariableReferenceExpression, String> variableToColumnName)
    {
        ImmutableMap.Builder<VariableReferenceExpression, com.facebook.presto.spi.ColumnHandle> assignments = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, String> entry : variableToColumnName.entrySet()) {
            assignments.put(entry.getKey(), new TestingColumnHandle(entry.getValue()));
        }
        return new TableScanNode(
                Optional.empty(), scanId,
                new TableHandle(new ConnectorId("test"), new TestingTableHandle(),
                        TestingTransactionHandle.create(), Optional.empty()),
                variables, assignments.build(),
                TupleDomain.all(), TupleDomain.all(), Optional.empty());
    }

    private static final long DEFAULT_MAX_SIZE_BYTES = 1_048_576L; // 1 MB

    private JoinDynamicFilter createTestFilter()
    {
        return new JoinDynamicFilter(
                "",
                "",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterStats(),
                new RuntimeStats(),
                false);
    }

    private JoinDynamicFilter createTestFilterWithId(String filterId)
    {
        return new JoinDynamicFilter(
                filterId,
                "column_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterStats(),
                new RuntimeStats(),
                false);
    }
}
