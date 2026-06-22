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
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
        assertEquals(service.getStats().getFilterRegistrations().getTotalCount(), 1);
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
        assertEquals(service.getStats().getFilterRegistrations().getTotalCount(), 3);
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
