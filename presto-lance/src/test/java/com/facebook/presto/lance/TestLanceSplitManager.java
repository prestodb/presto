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
import com.google.common.io.Resources;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestLanceSplitManager
{
    private LanceNamespaceHolder namespaceHolder;
    private LanceSplitManager splitManager;
    private LanceTableHandle tableHandle;
    private int fragmentCount;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL dbUrl = Resources.getResource(TestLanceSplitManager.class, "/example_db");
        String rootPath = Paths.get(dbUrl.toURI()).toString();
        LanceConfig config = new LanceConfig()
                .setRootUrl(rootPath)
                .setSingleLevelNs(true);
        namespaceHolder = new LanceNamespaceHolder(config);
        splitManager = new LanceSplitManager(namespaceHolder);
        tableHandle = new LanceTableHandle("default", "test_table1");
        fragmentCount = namespaceHolder.getFragments("test_table1", Optional.empty()).size();
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
