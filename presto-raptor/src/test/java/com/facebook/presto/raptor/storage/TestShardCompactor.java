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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestShardCompactor
{
    private static final ConnectorSession SESSION = new ConnectorSession("presto_test", UTC_KEY, ENGLISH, System.currentTimeMillis(), null);
    private static final PagesIndexPageSorter PAGE_SORTER = new PagesIndexPageSorter();

    private OrcStorageManager storageManager;
    private File temporary;
    private Handle dummyHandle;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        temporary = createTempDir();
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        storageManager = createOrcStorageManager(dbi, temporary);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (dummyHandle != null) {
            dummyHandle.close();
        }
        deleteRecursively(temporary);
    }

    @Test
    public void testShardCompactor()
            throws Exception
    {
        ShardCompactor compactor = new ShardCompactor(storageManager);

        List<Long> columnIds = ImmutableList.of(3L, 7L, 2L, 1L, 5L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, DATE, TIMESTAMP);

        // MAX_SHARD_ROWS = 100, so 3 shards of 50 rows each should be compacted to 2 shards
        List<ShardInfo> inputShards = createShards(storageManager, columnIds, columnTypes, 3, 50);
        Set<UUID> inputUuids = ImmutableSet.copyOf(inputShards.stream().map(ShardInfo::getShardUuid).collect(toList()));
        assertEquals(inputShards.size(), 3);

        List<ShardInfo> outputShards = compactor.compact(inputUuids, getColumnInfo(columnIds, columnTypes));
        Set<UUID> outputUuids = ImmutableSet.copyOf(outputShards.stream().map(ShardInfo::getShardUuid).collect(toList()));
        assertEquals(outputShards.size(), 2);

        assertShardEqualsIgnoreOrder(inputUuids, outputUuids, columnIds, columnTypes);
    }

    @Test
    public void testShardCompactorSorted()
            throws Exception
    {
        ShardCompactor compactor = new ShardCompactor(storageManager);

        List<Long> columnIds = ImmutableList.of(3L, 7L, 2L, 1L, 5L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, DATE, TIMESTAMP);

        List<Long> sortColumnIds = ImmutableList.of(1L, 2L, 3L, 5L, 7L);
        List<SortOrder> sortOrders = nCopies(sortColumnIds.size(), ASC_NULLS_FIRST);
        List<Integer> sortIndexes = sortColumnIds.stream()
                .map(columnIds::indexOf)
                .collect(toList());

        // MAX_SHARD_ROWS = 100, so 10 shards of 10 rows each should be compacted to 1 shard
        List<ShardInfo> inputShards = createShardsSorted(storageManager, columnIds, columnTypes, sortIndexes, sortOrders, 10, 10);
        Set<UUID> inputUuids = ImmutableSet.copyOf(inputShards.stream()
                .map(ShardInfo::getShardUuid)
                .collect(toList()));
        assertEquals(inputShards.size(), 10);

        List<ShardInfo> outputShards = compactor.compactSorted(inputUuids, getColumnInfo(columnIds, columnTypes), sortColumnIds, sortOrders);
        Set<UUID> outputUuids = ImmutableSet.copyOf(outputShards.stream()
                .map(ShardInfo::getShardUuid)
                .collect(toList()));
        assertEquals(outputShards.size(), 1);

        assertShardEqualsSorted(inputUuids, outputUuids, columnIds, columnTypes, sortIndexes, sortOrders);
    }

    private void assertShardEqualsIgnoreOrder(Set<UUID> inputUuids, Set<UUID> outputUuids, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        MaterializedResult inputRows = getMaterializedRows(inputUuids, columnIds, columnTypes);
        MaterializedResult outputRows = getMaterializedRows(outputUuids, columnIds, columnTypes);
        assertEqualsIgnoreOrder(outputRows, inputRows);
    }

    private void assertShardEqualsSorted(Set<UUID> inputUuids, Set<UUID> outputUuids, List<Long> columnIds, List<Type> columnTypes, List<Integer> sortIndexes, List<SortOrder> sortOrders)
            throws IOException
    {
        List<Page> inputPages = getPages(inputUuids, columnIds, columnTypes);
        MaterializedResult inputRowsSorted = sortAndMaterialize(inputPages, columnTypes, sortIndexes, sortOrders);
        MaterializedResult outputRows = getMaterializedRows(outputUuids, columnIds, columnTypes);
        assertEquals(outputRows, inputRowsSorted);
    }

    private static MaterializedResult sortAndMaterialize(List<Page> pages, List<Type> columnTypes, List<Integer> sortIndexes, List<SortOrder> sortOrders)
    {
        long[] orderedAddresses = PAGE_SORTER.sort(columnTypes, pages, sortIndexes, sortOrders, 10_000);

        int pageIndex;
        int positionIndex;
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, columnTypes);
        PageBuilder pageBuilder = new PageBuilder(columnTypes);

        for (long orderedAddress : orderedAddresses) {
            pageIndex = PAGE_SORTER.decodePageIndex(orderedAddress);
            positionIndex = PAGE_SORTER.decodePositionIndex(orderedAddress);
            Page page = pages.get(pageIndex);
            for (int i = 0; i < columnTypes.size(); i++) {
                columnTypes.get(i).appendTo(page.getBlock(i), positionIndex, pageBuilder.getBlockBuilder(i));
            }
            pageBuilder.declarePosition();
        }
        resultBuilder.page(pageBuilder.build());

        return resultBuilder.build();
    }

    private List<Page> getPages(Set<UUID> uuids, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        ImmutableList.Builder<Page> builder = ImmutableList.builder();
        for (UUID uuid : uuids) {
            try (ConnectorPageSource pageSource = storageManager.getPageSource(uuid, columnIds, columnTypes, TupleDomain.all())) {
                while (!pageSource.isFinished()) {
                    Page outputPage = pageSource.getNextPage();
                    if (outputPage == null) {
                        break;
                    }
                    outputPage.assureLoaded();
                    builder.add(outputPage);
                }
            }
        }
        return builder.build();
    }

    private MaterializedResult getMaterializedRows(Set<UUID> uuids, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        MaterializedResult.Builder builder = MaterializedResult.resultBuilder(SESSION, columnTypes);
        for (UUID uuid : uuids) {
            try (ConnectorPageSource pageSource = storageManager.getPageSource(uuid, columnIds, columnTypes, TupleDomain.all())) {
                MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
                builder.rows(result.getMaterializedRows());
            }
        }
        return builder.build();
    }

    private static List<ShardInfo> createShardsSorted(StorageManager storageManager, List<Long> columnIds, List<Type> columnTypes, List<Integer> sortChannels, List<SortOrder> sortOrders, int count, int length)
    {
        StoragePageSink sink = storageManager.createStoragePageSink(columnIds, columnTypes);
        for (int numShards = 0; numShards < count; numShards++) {
            List<Page> pages = createPages(columnTypes, 1, length);
            long[] orderedAddresses = PAGE_SORTER.sort(columnTypes, pages, sortChannels, sortOrders, 10_000);
            int[] orderedPageIndex = new int[orderedAddresses.length];
            int[] orderedPositionIndex = new int[orderedAddresses.length];
            for (int i = 0; i < orderedAddresses.length; i++) {
                orderedPageIndex[i] = PAGE_SORTER.decodePageIndex(orderedAddresses[i]);
                orderedPositionIndex[i] = PAGE_SORTER.decodePositionIndex(orderedAddresses[i]);
            }
            sink.appendPages(pages, orderedPageIndex, orderedPositionIndex);
            sink.flush();
        }
        return sink.commit();
    }

    private static List<ShardInfo> createShards(StorageManager storageManager, List<Long> columnIds, List<Type> columnTypes, int count, int length)
    {
        List<Page> pages = createPages(columnTypes, count, length);
        StoragePageSink sink = storageManager.createStoragePageSink(columnIds, columnTypes);
        for (Page page : pages) {
            sink.appendPages(ImmutableList.of(page));
            sink.flush();
        }
        return sink.commit();
    }

    private static List<Page> createPages(List<Type> columnTypes, int count, int length)
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            pages.add(SequencePageBuilder.createSequencePage(columnTypes, length));
        }
        return pages.build();
    }

    private static List<ColumnInfo> getColumnInfo(List<Long> columnIds, List<Type> columnTypes)
    {
        ImmutableList.Builder<ColumnInfo> columnInfoBuilder = ImmutableList.builder();
        for (int i = 0; i < columnIds.size(); i++) {
            columnInfoBuilder.add(new ColumnInfo(columnIds.get(i), columnTypes.get(i)));
        }
        return columnInfoBuilder.build();
    }
}
