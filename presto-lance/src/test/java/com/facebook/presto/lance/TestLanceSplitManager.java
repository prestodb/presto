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
package com.facebook.presto.lance;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.lance.Fragment;
import org.lance.index.Index;
import org.lance.index.IndexType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestLanceSplitManager
{
    private LanceNamespaceHolder namespaceHolder;
    private LanceSplitManager splitManager;
    private LanceTableHandle tableHandle;
    private int fragmentCount;

    // vector_db-backed handles for vector search split tests
    private LanceNamespaceHolder vectorDbNamespaceHolder;
    private LanceSplitManager vectorDbSplitManager;
    private LanceTableHandle vectorDbTableHandle;
    private int vectorDbFragmentCount;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL dbUrl = Resources.getResource(TestLanceSplitManager.class, "/example_db");
        String rootPath = Paths.get(dbUrl.toURI()).toString();
        LanceConfig config = new LanceConfig()
                .setSingleLevelNs(true);
        Map<String, String> namespaceProperties = ImmutableMap.of("lance.root", rootPath);
        namespaceHolder = new LanceNamespaceHolder(config, namespaceProperties);
        splitManager = new LanceSplitManager(namespaceHolder);

        String tablePath = namespaceHolder.getTablePath("default", "test_table1");
        List<String> tableId = namespaceHolder.getTableId("default", "test_table1");
        long datasetVersion = namespaceHolder.getLatestVersion(tablePath);
        tableHandle = new LanceTableHandle("default", "test_table1", tablePath, tableId, Optional.of(datasetVersion));
        fragmentCount = namespaceHolder.getFragments(tablePath, Optional.of(datasetVersion)).size();

        // Set up vector_db namespace holder for vector search split tests
        URL vectorDbUrl = Resources.getResource(TestLanceSplitManager.class, "/vector_db");
        String vectorRootPath = Paths.get(vectorDbUrl.toURI()).toString();
        LanceConfig vectorConfig = new LanceConfig().setSingleLevelNs(true);
        vectorDbNamespaceHolder = new LanceNamespaceHolder(vectorConfig, ImmutableMap.of("lance.root", vectorRootPath));
        vectorDbSplitManager = new LanceSplitManager(vectorDbNamespaceHolder);

        String vectorTablePath = vectorDbNamespaceHolder.getTablePath("default", "vectors");
        List<String> vectorTableId = vectorDbNamespaceHolder.getTableId("default", "vectors");
        long vectorDatasetVersion = vectorDbNamespaceHolder.getLatestVersion(vectorTablePath);
        vectorDbTableHandle = new LanceTableHandle("default", "vectors", vectorTablePath, vectorTableId, Optional.of(vectorDatasetVersion));
        vectorDbFragmentCount = vectorDbNamespaceHolder.getFragments(vectorTablePath, Optional.of(vectorDatasetVersion)).size();
    }

    @AfterMethod
    public void tearDown()
    {
        namespaceHolder.shutdown();
        vectorDbNamespaceHolder.shutdown();
    }

    @Test
    public void testGetSplitsWithoutLimitProducesSplitPerFragment()
    {
        List<ConnectorSplit> splits = drain(splitManager.getSplits(
                null, null, new LanceTableLayoutHandle(tableHandle, TupleDomain.all()), null));
        assertEquals(splits.size(), fragmentCount);
    }

    @Test
    public void testGetSplitsWithLimitProducesSingleCoalescedSplit()
    {
        List<ConnectorSplit> splits = drain(splitManager.getSplits(
                null, null, new LanceTableLayoutHandle(tableHandle.withLimit(1), TupleDomain.all()), null));
        assertEquals(splits.size(), 1);
        // the coalesced split covers the limit using at least one fragment
        assertTrue(((LanceSplit) splits.get(0)).getFragments().size() >= 1);
    }

    @Test
    public void testCoalesceStopsAtFirstFragmentCoveringLimit()
    {
        List<Integer> selected = LanceSplitManager.coalesceFragmentsForLimit(
                ImmutableList.of(0, 1, 2), ImmutableList.of(100L, 100L, 100L), 50);
        assertEquals(selected, ImmutableList.of(0));
    }

    @Test
    public void testCoalesceSpansMultipleFragments()
    {
        List<Integer> selected = LanceSplitManager.coalesceFragmentsForLimit(
                ImmutableList.of(0, 1, 2), ImmutableList.of(100L, 100L, 100L), 150);
        assertEquals(selected, ImmutableList.of(0, 1));
    }

    @Test
    public void testCoalesceLimitExceedsTotalReturnsAll()
    {
        List<Integer> selected = LanceSplitManager.coalesceFragmentsForLimit(
                ImmutableList.of(0, 1, 2), ImmutableList.of(100L, 100L, 100L), 1000);
        assertEquals(selected, ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testCoalesceWalksPastDeletedFragments()
    {
        // Fully-deleted fragments contribute 0 rows; the walk continues until the limit is covered
        List<Integer> selected = LanceSplitManager.coalesceFragmentsForLimit(
                ImmutableList.of(0, 1, 2), ImmutableList.of(0L, 0L, 5L), 3);
        assertEquals(selected, ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testCoalesceAlwaysReturnsAtLeastOneFragment()
    {
        List<Integer> selected = LanceSplitManager.coalesceFragmentsForLimit(
                ImmutableList.of(7), ImmutableList.of(0L), 10);
        assertEquals(selected, ImmutableList.of(7));
    }

    @Test
    public void testCoalesceEmptyFragments()
    {
        List<Integer> selected = LanceSplitManager.coalesceFragmentsForLimit(
                ImmutableList.of(), ImmutableList.of(), 10);
        assertEquals(selected, ImmutableList.of());
    }

    @Test
    public void testVectorSearchSplitsOneSplitPerSegment()
    {
        // Two disjoint segments covering all fragments: one split each, no uncovered split.
        List<LanceSplit> splits = LanceSplitManager.buildVectorSearchSplits(
                "p",
                ImmutableList.of(0, 1, 2, 3),
                ImmutableList.of(ImmutableList.of(0, 1), ImmutableList.of(2, 3)));
        assertEquals(splits.size(), 2);
        assertEquals(splits.get(0).getFragments(), ImmutableList.of(0, 1));
        assertEquals(splits.get(1).getFragments(), ImmutableList.of(2, 3));
    }

    @Test
    public void testVectorSearchSplitsAddsUncoveredFragmentSplit()
    {
        // Fragments 4 and 5 are not covered by any segment -> one extra flat-search split.
        List<LanceSplit> splits = LanceSplitManager.buildVectorSearchSplits(
                "p",
                ImmutableList.of(0, 1, 2, 3, 4, 5),
                ImmutableList.of(ImmutableList.of(0, 1), ImmutableList.of(2, 3)));
        assertEquals(splits.size(), 3);
        assertEquals(splits.get(2).getFragments(), ImmutableList.of(4, 5));
    }

    @Test
    public void testVectorSearchSplitsNoSegmentsScansAllFragments()
    {
        // No vector index segments -> single split over all fragments (Lance flat-searches).
        List<LanceSplit> splits = LanceSplitManager.buildVectorSearchSplits(
                "p",
                ImmutableList.of(0, 1, 2),
                ImmutableList.of());
        assertEquals(splits.size(), 1);
        assertEquals(splits.get(0).getFragments(), ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testVectorSearchSplitsSkipsEmptySegments()
    {
        // An empty segment contributes no split; uncovered fragments still get one.
        List<LanceSplit> splits = LanceSplitManager.buildVectorSearchSplits(
                "p",
                ImmutableList.of(0, 1),
                ImmutableList.of(ImmutableList.of(), ImmutableList.of(0)));
        assertEquals(splits.size(), 2);
        assertEquals(splits.get(0).getFragments(), ImmutableList.of(0));
        assertEquals(splits.get(1).getFragments(), ImmutableList.of(1));
    }

    @Test
    public void testVectorSearchGlobalTopKProducesSingleSplit()
    {
        // When requiresGlobalTopK is true (TopN rewriter never ran), getSplits must return
        // exactly one split containing all fragment ids so a single Lance nearest query
        // computes the exact global top-k.
        LanceVectorSearch vectorSearch = new LanceVectorSearch(
                "vec",
                ImmutableList.of(0.3, 0.0, 0.0, 0.0),
                3,
                LanceVectorSearch.METRIC_L2,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true);
        LanceTableHandle handle = vectorDbTableHandle.withVectorSearch(vectorSearch);

        List<ConnectorSplit> splits = drain(vectorDbSplitManager.getSplits(
                null, null, new LanceTableLayoutHandle(handle, TupleDomain.all()), null));
        assertEquals(splits.size(), 1);

        LanceSplit single = (LanceSplit) splits.get(0);
        // The single split must cover every fragment in the dataset
        List<Integer> allFragmentIds = vectorDbNamespaceHolder.getFragments(
                        vectorDbTableHandle.getTablePath(),
                        vectorDbTableHandle.getDatasetVersion()).stream()
                .map(Fragment::getId)
                .collect(toImmutableList());
        assertEquals(single.getFragments(), allFragmentIds);
    }

    @Test
    public void testVectorSearchWithoutGlobalTopKAlsoSingleSplit()
    {
        // When requiresGlobalTopK is false (TopN rewriter already cleared it), getSplits
        // still returns a single split covering all fragments. lance-core (verified through
        // 11.0.0) does not support fragment-scoped nearest, so per-segment fan-out is disabled.
        LanceVectorSearch vectorSearch = new LanceVectorSearch(
                "vec",
                ImmutableList.of(0.3, 0.0, 0.0, 0.0),
                3,
                LanceVectorSearch.METRIC_L2,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false);
        LanceTableHandle handle = vectorDbTableHandle.withVectorSearch(vectorSearch);

        List<ConnectorSplit> splits = drain(vectorDbSplitManager.getSplits(
                null, null, new LanceTableLayoutHandle(handle, TupleDomain.all()), null));
        assertEquals(splits.size(), 1);

        LanceSplit single = (LanceSplit) splits.get(0);
        // The single split must cover every fragment in the dataset
        List<Integer> allFragmentIds = vectorDbNamespaceHolder.getFragments(
                        vectorDbTableHandle.getTablePath(),
                        vectorDbTableHandle.getDatasetVersion()).stream()
                .map(Fragment::getId)
                .collect(toImmutableList());
        assertEquals(single.getFragments(), allFragmentIds);
    }

    @Test
    public void testSegmentFragmentLists()
    {
        // Two segments with disjoint fragments -> returns both lists
        List<Index> segments = ImmutableList.of(
                Index.builder()
                        .uuid(UUID.randomUUID())
                        .name("seg0")
                        .fields(ImmutableList.of(0))
                        .datasetVersion(1)
                        .indexType(IndexType.IVF_PQ)
                        .fragments(ImmutableList.of(0, 1, 2))
                        .build(),
                Index.builder()
                        .uuid(UUID.randomUUID())
                        .name("seg1")
                        .fields(ImmutableList.of(0))
                        .datasetVersion(1)
                        .indexType(IndexType.IVF_PQ)
                        .fragments(ImmutableList.of(3, 4))
                        .build());
        List<List<Integer>> result = LanceSplitManager.segmentFragmentLists(segments);
        assertEquals(result.size(), 2);
        assertEquals(result.get(0), ImmutableList.of(0, 1, 2));
        assertEquals(result.get(1), ImmutableList.of(3, 4));
    }

    @Test
    public void testSegmentFragmentListsMissingCoverageFallsBack()
    {
        // One segment has fragments, the other has none -> falls back to empty list
        List<Index> segments = ImmutableList.of(
                Index.builder()
                        .uuid(UUID.randomUUID())
                        .name("seg0")
                        .fields(ImmutableList.of(0))
                        .datasetVersion(1)
                        .indexType(IndexType.IVF_PQ)
                        .fragments(ImmutableList.of(0, 1))
                        .build(),
                Index.builder()
                        .uuid(UUID.randomUUID())
                        .name("seg1")
                        .fields(ImmutableList.of(0))
                        .datasetVersion(1)
                        .indexType(IndexType.IVF_PQ)
                        .build());
        List<List<Integer>> result = LanceSplitManager.segmentFragmentLists(segments);
        assertEquals(result, ImmutableList.of());
    }

    @Test
    public void testSegmentFragmentListsOverlapFallsBack()
    {
        // Fragments overlap (fragment 1 in both segments) -> falls back to empty list
        List<Index> segments = ImmutableList.of(
                Index.builder()
                        .uuid(UUID.randomUUID())
                        .name("seg0")
                        .fields(ImmutableList.of(0))
                        .datasetVersion(1)
                        .indexType(IndexType.IVF_PQ)
                        .fragments(ImmutableList.of(0, 1))
                        .build(),
                Index.builder()
                        .uuid(UUID.randomUUID())
                        .name("seg1")
                        .fields(ImmutableList.of(0))
                        .datasetVersion(1)
                        .indexType(IndexType.IVF_PQ)
                        .fragments(ImmutableList.of(1, 2))
                        .build());
        List<List<Integer>> result = LanceSplitManager.segmentFragmentLists(segments);
        assertEquals(result, ImmutableList.of());
    }

    private static List<ConnectorSplit> drain(ConnectorSplitSource source)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        try {
            while (!source.isFinished()) {
                splits.addAll(source.getNextBatch(NOT_PARTITIONED, 1000).get().getSplits());
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return splits.build();
    }
}
