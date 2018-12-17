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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.storage.OrcStorageManager;
import com.facebook.presto.raptorx.storage.ReaderAttributes;
import com.facebook.presto.raptorx.storage.StorageManager;
import com.facebook.presto.raptorx.storage.StoragePageSink;
import com.facebook.presto.raptorx.util.TestingDatabase;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.raptorx.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@Test(singleThreaded = true)
public class TestChunkCompactor
{
    private static final int MAX_CHUNK_ROWS = 1000;
    private static final PagesIndexPageSorter PAGE_SORTER = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
    private static final ReaderAttributes READER_ATTRIBUTES = new ReaderAttributes(new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));

    private OrcStorageManager storageManager;
    private ChunkCompactor compactor;
    private File temporary;
    private TestingDatabase database;
    private static final long tableId = 123;
    private static final int bucketNumber = 10;
    private static final long transactionId = 1;
    @BeforeMethod
    public void setup()
    {
        temporary = createTempDir();
        database = new TestingDatabase();
        storageManager = createOrcStorageManager(database, temporary, MAX_CHUNK_ROWS);
        compactor = new ChunkCompactor(storageManager, READER_ATTRIBUTES);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (database != null) {
            database.close();
        }
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testChunkCompactor()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L, 7L, 2L, 1L, 5L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(20), DOUBLE, DATE, TIMESTAMP);

        List<ChunkInfo> inputChunks = createChunks(storageManager, columnIds, columnTypes, 3);
        assertEquals(inputChunks.size(), 3);

        long totalRows = inputChunks.stream()
                .mapToLong(ChunkInfo::getRowCount)
                .sum();
        long expectedOutputChunks = computeExpectedOutputChunks(totalRows);

        Set<Long> inputChunkIds = inputChunks.stream().map(ChunkInfo::getChunkId).collect(toSet());

        List<ChunkInfo> outputChunks = compactor.compact(transactionId, tableId, bucketNumber, inputChunkIds, getColumnInfo(columnIds, columnTypes), CompressionType.ZSTD);
        assertEquals(outputChunks.size(), expectedOutputChunks);

        Set<Long> outputChunkIds = outputChunks.stream().map(ChunkInfo::getChunkId).collect(toSet());
        assertChunkEqualsIgnoreOrder(inputChunkIds, outputChunkIds, columnIds, columnTypes);
    }

    @Test
    public void testChunkCompactorSorted()
            throws Exception
    {
        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(20), DATE, TIMESTAMP, DOUBLE);
        List<Long> columnIds = ImmutableList.of(3L, 7L, 2L, 1L, 5L);
        List<Long> sortColumnIds = ImmutableList.of(1L, 2L, 3L, 5L, 7L);
        List<SortOrder> sortOrders = nCopies(sortColumnIds.size(), ASC_NULLS_FIRST);
        List<Integer> sortIndexes = sortColumnIds.stream()
                .map(columnIds::indexOf)
                .collect(toList());

        List<ChunkInfo> inputChunks = createSortedChunks(storageManager, columnIds, columnTypes, sortIndexes, sortOrders, 2);
        assertEquals(inputChunks.size(), 2);

        long totalRows = inputChunks.stream().mapToLong(ChunkInfo::getRowCount).sum();
        long expectedOutputChunks = computeExpectedOutputChunks(totalRows);

        Set<Long> inputChunkIds = inputChunks.stream().map(ChunkInfo::getChunkId).collect(toSet());

        List<ChunkInfo> outputChunks = compactor.compactSorted(transactionId, tableId, bucketNumber, inputChunkIds, getColumnInfo(columnIds, columnTypes), sortColumnIds, sortOrders, CompressionType.ZSTD);
        List<Long> outputUuids = outputChunks.stream()
                .map(ChunkInfo::getChunkId)
                .collect(toList());
        assertEquals(outputChunks.size(), expectedOutputChunks);

        assertChunkEqualsSorted(inputChunkIds, outputUuids, columnIds, columnTypes, sortIndexes, sortOrders);
    }

    private static long computeExpectedOutputChunks(long totalRows)
    {
        return ((totalRows % MAX_CHUNK_ROWS) != 0) ? ((totalRows / MAX_CHUNK_ROWS) + 1) : (totalRows / MAX_CHUNK_ROWS);
    }

    private void assertChunkEqualsIgnoreOrder(Set<Long> inputChunkIds, Set<Long> outputChunkIds, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        MaterializedResult inputRows = getMaterializedRows(ImmutableList.copyOf(inputChunkIds), columnIds, columnTypes);
        MaterializedResult outputRows = getMaterializedRows(ImmutableList.copyOf(outputChunkIds), columnIds, columnTypes);

        assertEqualsIgnoreOrder(outputRows, inputRows);
    }

    private void assertChunkEqualsSorted(Set<Long> inputChunkIds, List<Long> outputChunkIds, List<Long> columnIds, List<Type> columnTypes, List<Integer> sortIndexes, List<SortOrder> sortOrders)
            throws IOException
    {
        List<Page> inputPages = getPages(inputChunkIds, columnIds, columnTypes);
        List<Type> sortTypes = sortIndexes.stream().map(columnTypes::get).collect(toList());

        MaterializedResult inputRowsSorted = sortAndMaterialize(inputPages, columnTypes, sortIndexes, sortOrders, sortTypes);
        MaterializedResult outputRows = extractColumns(getMaterializedRows(outputChunkIds, columnIds, columnTypes), sortIndexes, sortTypes);

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
        Page buildPage = pageBuilder.build();
        Block[] outputBlocks = new Block[buildPage.getChannelCount()];

        for (int i = 0; i < sortIndexes.size(); i++) {
            outputBlocks[i] = buildPage.getBlock(sortIndexes.get(i));
        }

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, sortTypes);
        resultBuilder.page(new Page(outputBlocks));

        return resultBuilder.build();
    }

    private List<Page> getPages(Set<Long> chunkIds, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (long chunkId : chunkIds) {
            try (ConnectorPageSource pageSource = getPageSource(columnIds, columnTypes, chunkId)) {
                while (!pageSource.isFinished()) {
                    Page outputPage = pageSource.getNextPage();
                    if (outputPage == null) {
                        break;
                    }
                    pages.add(outputPage.getLoadedPage());
                }
            }
        }
        return pages.build();
    }

    private MaterializedResult getMaterializedRows(List<Long> chunkIds, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        MaterializedResult.Builder rows = MaterializedResult.resultBuilder(SESSION, columnTypes);
        for (long chunkId : chunkIds) {
            try (ConnectorPageSource pageSource = getPageSource(columnIds, columnTypes, chunkId)) {
                MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, columnTypes);
                rows.rows(result.getMaterializedRows());
            }
        }
        return rows.build();
    }

    private ConnectorPageSource getPageSource(List<Long> columnIds, List<Type> columnTypes, long chunkId)
    {
        Map<Long, Type> chunkColumnTypes = new HashMap<>();
        for (int i = 0; i < columnIds.size(); i++) {
            chunkColumnTypes.put(columnIds.get(i), columnTypes.get(i));
        }
        return storageManager.getPageSource(tableId, chunkId, bucketNumber, columnIds, columnTypes, TupleDomain.all(), READER_ATTRIBUTES, OptionalLong.of(1L), chunkColumnTypes, Optional.of(CompressionType.ZSTD));
    }

    private static List<ChunkInfo> createSortedChunks(StorageManager storageManager, List<Long> columnIds, List<Type> columnTypes, List<Integer> sortChannels, List<SortOrder> sortOrders, int chunkCount)
    {
        StoragePageSink sink = createStoragePageSink(storageManager, columnIds, columnTypes);
        for (int chunkNum = 0; chunkNum < chunkCount; chunkNum++) {
            createSortedChunk(columnTypes, sortChannels, sortOrders, sink);
        }
        return getFutureValue(sink.commit());
    }

    private static void createSortedChunk(List<Type> columnTypes, List<Integer> sortChannels, List<SortOrder> sortOrders, StoragePageSink sink)
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

    private static List<ChunkInfo> createChunks(StorageManager storageManager, List<Long> columnIds, List<Type> columnTypes, int chunkCount)
    {
        StoragePageSink sink = createStoragePageSink(storageManager, columnIds, columnTypes);
        for (int i = 0; i < chunkCount; i++) {
            sink.appendPages(createPages(columnTypes));
            sink.flush();
        }
        return getFutureValue(sink.commit());
    }

    private static StoragePageSink createStoragePageSink(StorageManager manager, List<Long> columnIds, List<Type> columnTypes)
    {
        long transactionId = 1;
        return manager.createStoragePageSink(transactionId, tableId, bucketNumber, columnIds, columnTypes, CompressionType.ZSTD);
    }

    private static List<Page> createPages(List<Type> columnTypes)
    {
        // Creates 10 pages with 10 rows each
        int rowCount = 10;
        int pageCount = 10;

        // some random values to start off the blocks
        int[][] initialValues = {{17, 15, 16, 18, 14}, {59, 55, 54, 53, 58}};

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
            columnInfos.add(new ColumnInfo(columnIds.get(i), String.valueOf(columnIds.get(i)), columnTypes.get(i), Optional.empty(), i + 1, OptionalInt.empty(), OptionalInt.empty()));
        }
        return columnInfos.build();
    }
}
