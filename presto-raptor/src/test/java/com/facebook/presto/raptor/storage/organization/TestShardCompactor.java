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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.OrcStorageManager;
import com.facebook.presto.raptor.storage.ReaderAttributes;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StoragePageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestShardCompactor
{
    private static final int MAX_SHARD_ROWS = 1000;
    private static final PagesIndexPageSorter PAGE_SORTER = new PagesIndexPageSorter();
    private static final ReaderAttributes READER_ATTRIBUTES = new ReaderAttributes(new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));

    private OrcStorageManager storageManager;
    private ShardCompactor compactor;
    private File temporary;
    private Handle dummyHandle;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        temporary = createTempDir();
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        storageManager = createOrcStorageManager(dbi, temporary, MAX_SHARD_ROWS);
        compactor = new ShardCompactor(storageManager, READER_ATTRIBUTES);
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
        List<Long> columnIds = ImmutableList.of(3L, 7L, 2L, 1L, 5L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(20), DOUBLE, DATE, TIMESTAMP);

        List<ShardInfo> inputShards = createShards(storageManager, columnIds, columnTypes, 3);
        assertEquals(inputShards.size(), 3);

        long totalRows = inputShards.stream()
                .mapToLong(ShardInfo::getRowCount)
                .sum();
        long expectedOutputShards = computeExpectedOutputShards(totalRows);

        Set<UUID> inputUuids = inputShards.stream().map(ShardInfo::getShardUuid).collect(toSet());

        long transactionId = 1;
        List<ShardInfo> outputShards = compactor.compact(transactionId, OptionalInt.empty(), inputUuids, getColumnInfo(columnIds, columnTypes));
        assertEquals(outputShards.size(), expectedOutputShards);

        Set<UUID> outputUuids = outputShards.stream().map(ShardInfo::getShardUuid).collect(toSet());
        assertShardEqualsIgnoreOrder(inputUuids, outputUuids, columnIds, columnTypes);
    }

    @Test
    public void testShardCompactorSorted()
            throws Exception
    {
        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(20), DATE, TIMESTAMP, DOUBLE);
        List<Long> columnIds = ImmutableList.of(3L, 7L, 2L, 1L, 5L);
        List<Long> sortColumnIds = ImmutableList.of(1L, 2L, 3L, 5L, 7L);
        List<SortOrder> sortOrders = nCopies(sortColumnIds.size(), ASC_NULLS_FIRST);
        List<Integer> sortIndexes = sortColumnIds.stream()
                .map(columnIds::indexOf)
                .collect(toList());

        List<ShardInfo> inputShards = createSortedShards(storageManager, columnIds, columnTypes, sortIndexes, sortOrders, 2);
        assertEquals(inputShards.size(), 2);

        long totalRows = inputShards.stream().mapToLong(ShardInfo::getRowCount).sum();
        long expectedOutputShards = computeExpectedOutputShards(totalRows);

        Set<UUID> inputUuids = inputShards.stream().map(ShardInfo::getShardUuid).collect(toSet());

        long transactionId = 1;
        List<ShardInfo> outputShards = compactor.compactSorted(transactionId, OptionalInt.empty(), inputUuids, getColumnInfo(columnIds, columnTypes), sortColumnIds, sortOrders);
        List<UUID> outputUuids = outputShards.stream()
                .map(ShardInfo::getShardUuid)
                .collect(toList());
        assertEquals(outputShards.size(), expectedOutputShards);

        assertShardEqualsSorted(inputUuids, outputUuids, columnIds, columnTypes, sortIndexes, sortOrders);
    }

    private static long computeExpectedOutputShards(long totalRows)
    {
        return ((totalRows % MAX_SHARD_ROWS) != 0) ? ((totalRows / MAX_SHARD_ROWS) + 1) : (totalRows / MAX_SHARD_ROWS);
    }

    private void assertShardEqualsIgnoreOrder(Set<UUID> inputUuids, Set<UUID> outputUuids, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        MaterializedResult inputRows = getMaterializedRows(ImmutableList.copyOf(inputUuids), columnIds, columnTypes);
        MaterializedResult outputRows = getMaterializedRows(ImmutableList.copyOf(outputUuids), columnIds, columnTypes);

        assertEqualsIgnoreOrder(outputRows, inputRows);
    }

    private void assertShardEqualsSorted(Set<UUID> inputUuids, List<UUID> outputUuids, List<Long> columnIds, List<Type> columnTypes, List<Integer> sortIndexes, List<SortOrder> sortOrders)
            throws IOException
    {
        List<Page> inputPages = getPages(inputUuids, columnIds, columnTypes);
        List<Type> sortTypes = sortIndexes.stream().map(columnTypes::get).collect(toList());

        MaterializedResult inputRowsSorted = sortAndMaterialize(inputPages, columnTypes, sortIndexes, sortOrders, sortTypes);
        MaterializedResult outputRows = extractColumns(getMaterializedRows(outputUuids, columnIds, columnTypes), sortIndexes, sortTypes);

        assertEquals(outputRows, inputRowsSorted);
    }

    private static MaterializedResult extractColumns(MaterializedResult materializedRows, List<Integer> indexes, List<Type> types)
    {
        ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();
        for (MaterializedRow row : materializedRows) {
            Object[] values = new Object[indexes.size()];
            for (int i = 0; i < indexes.size(); i++) {
                values[i] = row.getField(indexes.get(i));
            }
            rows.add(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, values));
        }
        return new MaterializedResult(rows.build(), types);
    }

    private static MaterializedResult sortAndMaterialize(List<Page> pages, List<Type> columnTypes, List<Integer> sortIndexes, List<SortOrder> sortOrders, List<Type> sortTypes)
    {
        long[] orderedAddresses = PAGE_SORTER.sort(columnTypes, pages, sortIndexes, sortOrders, 10_000);

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        for (long orderedAddress : orderedAddresses) {
            int pageIndex = PAGE_SORTER.decodePageIndex(orderedAddress);
            int positionIndex = PAGE_SORTER.decodePositionIndex(orderedAddress);

            Page page = pages.get(pageIndex);
            pageBuilder.declarePosition();
            for (int i = 0; i < columnTypes.size(); i++) {
                columnTypes.get(i).appendTo(page.getBlock(i), positionIndex, pageBuilder.getBlockBuilder(i));
            }
        }

        // extract the sortIndexes and reorder the blocks by sort indexes (useful for debugging)
        Block[] blocks = pageBuilder.build().getBlocks();
        Block[] outputBlocks = new Block[blocks.length];

        for (int i = 0; i < sortIndexes.size(); i++) {
            outputBlocks[i] = blocks[sortIndexes.get(i)];
        }

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, sortTypes);
        resultBuilder.page(new Page(outputBlocks));

        return resultBuilder.build();
    }

    private List<Page> getPages(Set<UUID> uuids, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (UUID uuid : uuids) {
            try (ConnectorPageSource pageSource = getPageSource(columnIds, columnTypes, uuid)) {
                while (!pageSource.isFinished()) {
                    Page outputPage = pageSource.getNextPage();
                    if (outputPage == null) {
                        break;
                    }
                    outputPage.assureLoaded();
                    pages.add(outputPage);
                }
            }
        }
        return pages.build();
    }

    private MaterializedResult getMaterializedRows(List<UUID> uuids, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        MaterializedResult.Builder rows = MaterializedResult.resultBuilder(SESSION, columnTypes);
        for (UUID uuid : uuids) {
            try (ConnectorPageSource pageSource = getPageSource(columnIds, columnTypes, uuid)) {
                MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
                rows.rows(result.getMaterializedRows());
            }
        }
        return rows.build();
    }

    private ConnectorPageSource getPageSource(List<Long> columnIds, List<Type> columnTypes, UUID uuid)
    {
        return storageManager.getPageSource(uuid, OptionalInt.empty(), columnIds, columnTypes, TupleDomain.all(), READER_ATTRIBUTES);
    }

    private static List<ShardInfo> createSortedShards(StorageManager storageManager, List<Long> columnIds, List<Type> columnTypes, List<Integer> sortChannels, List<SortOrder> sortOrders, int shardCount)
    {
        StoragePageSink sink = createStoragePageSink(storageManager, columnIds, columnTypes);
        for (int shardNum = 0; shardNum < shardCount; shardNum++) {
            createSortedShard(columnTypes, sortChannels, sortOrders, sink);
        }
        return getFutureValue(sink.commit());
    }

    private static void createSortedShard(List<Type> columnTypes, List<Integer> sortChannels, List<SortOrder> sortOrders, StoragePageSink sink)
    {
        List<Page> pages = createPages(columnTypes);

        // Sort pages
        long[] orderedAddresses = PAGE_SORTER.sort(columnTypes, pages, sortChannels, sortOrders, 10_000);
        int[] orderedPageIndex = new int[orderedAddresses.length];
        int[] orderedPositionIndex = new int[orderedAddresses.length];

        for (int i = 0; i < orderedAddresses.length; i++) {
            orderedPageIndex[i] = PAGE_SORTER.decodePageIndex(orderedAddresses[i]);
            orderedPositionIndex[i] = PAGE_SORTER.decodePositionIndex(orderedAddresses[i]);
        }

        // Append sorted pages
        sink.appendPages(pages, orderedPageIndex, orderedPositionIndex);
        sink.flush();
    }

    private static List<ShardInfo> createShards(StorageManager storageManager, List<Long> columnIds, List<Type> columnTypes, int shardCount)
    {
        StoragePageSink sink = createStoragePageSink(storageManager, columnIds, columnTypes);
        for (int i = 0; i < shardCount; i++) {
            sink.appendPages(createPages(columnTypes));
            sink.flush();
        }
        return getFutureValue(sink.commit());
    }

    private static StoragePageSink createStoragePageSink(StorageManager manager, List<Long> columnIds, List<Type> columnTypes)
    {
        long transactionId = 1;
        return manager.createStoragePageSink(transactionId, OptionalInt.empty(), columnIds, columnTypes, false);
    }

    private static List<Page> createPages(List<Type> columnTypes)
    {
        // Creates 10 pages with 10 rows each
        int rowCount = 10;
        int pageCount = 10;

        // some random values to start off the blocks
        int[][] initialValues = { { 17, 15, 16, 18, 14 }, { 59, 55, 54, 53, 58 } };

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (int i = 0; i < pageCount; i++) {
            pages.add(SequencePageBuilder.createSequencePage(columnTypes, rowCount, initialValues[i % 2]));
        }
        return pages.build();
    }

    private static List<ColumnInfo> getColumnInfo(List<Long> columnIds, List<Type> columnTypes)
    {
        ImmutableList.Builder<ColumnInfo> columnInfos = ImmutableList.builder();
        for (int i = 0; i < columnIds.size(); i++) {
            columnInfos.add(new ColumnInfo(columnIds.get(i), columnTypes.get(i)));
        }
        return columnInfos.build();
    }
}
