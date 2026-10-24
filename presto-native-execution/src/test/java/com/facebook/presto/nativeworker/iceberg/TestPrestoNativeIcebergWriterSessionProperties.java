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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.facebook.presto.SystemSessionProperties.SCALE_WRITERS;
import static com.facebook.presto.SystemSessionProperties.TASK_WRITER_COUNT;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.NATIVE_PARQUET_WRITER_PAGE_MAX_ROWS;
import static com.facebook.presto.iceberg.IcebergSessionProperties.NATIVE_PARQUET_WRITER_ROW_GROUP_MAX_ROWS;
import static com.facebook.presto.iceberg.IcebergSessionProperties.TARGET_MAX_FILE_SIZE;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/// Verifies that the Iceberg writer session properties take effect on native workers.
///
/// Both limits are checked after each input batch, so a file or row group can overshoot its
/// target by up to one batch. The bounds below allow for that.
public class TestPrestoNativeIcebergWriterSessionProperties
        extends AbstractTestQueryFramework
{
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final long PAGE_TARGET_BYTES = 64L << 10;
    private static final String COMPRESSION_CODEC = "compression_codec";
    private static final long TARGET_BYTES = 1L << 20;
    private static final String TARGET = "1MB";
    private static final long ROW_COUNT = 500_000;
    // Hex digests do not compress well, so the data stays around 10MB once written.
    private static final String SOURCE_QUERY = "SELECT x * 1000 + y AS id, " +
            "to_hex(md5(to_utf8(CAST(x * 1000 + y AS VARCHAR)))) AS payload " +
            "FROM UNNEST(sequence(0, 499)) t(x) CROSS JOIN UNNEST(sequence(0, 999)) u(y)";
    // About 430MB once written without compression, enough for row groups above the 128MB default.
    private static final String LARGE_SOURCE_QUERY = "SELECT x * 1000 + y AS id, " +
            "concat(to_hex(md5(to_utf8(CAST(x * 1000 + y AS VARCHAR)))), to_hex(md5(to_utf8(CAST(x AS VARCHAR))))) AS payload " +
            "FROM UNNEST(sequence(0, 5999)) t(x) CROSS JOIN UNNEST(sequence(0, 999)) u(y)";
    private static final long LARGE_ROW_COUNT = 6_000_000;
    // Above both the 128MB default block size and the default cap of 1,048,576 rows per row group.
    private static final long LARGE_TARGET_BYTES = 160L << 20;
    private static final String LARGE_TARGET = "160MB";
    private static final long DEFAULT_ROW_GROUP_MAX_ROWS = 1L << 20;
    // Above the 1MB default page size.
    private static final long LARGE_PAGE_TARGET_BYTES = 4L << 20;
    private static final String UNSORTED = "";
    // Sorting by the digest reorders every row, so the sorted output differs from the input order.
    private static final String SORTED = "WITH (sorted_by = ARRAY['payload'])";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testTargetMaxFileSize()
    {
        writeTable("test_default_file_size", getSession(), UNSORTED);
        // Each writer produces one file by default.
        int writerCount = getFileSizes("test_default_file_size").size();

        writeTable("test_target_max_file_size", withIcebergProperty(TARGET_MAX_FILE_SIZE, TARGET), UNSORTED);
        assertFileSizesFollowTarget("test_target_max_file_size", writerCount);
    }

    @Test
    public void testTargetMaxFileSizeOnSortedTable()
    {
        // A sorted write buffers its rows and writes them out sorted when it finishes. It starts a new file as the
        // sorted output reaches the target.
        writeTable("test_sorted_default_file_size", getSession(), SORTED);
        int writerCount = getFileSizes("test_sorted_default_file_size").size();

        writeTable("test_sorted_target_max_file_size", withIcebergProperty(TARGET_MAX_FILE_SIZE, TARGET), SORTED);
        assertFileSizesFollowTarget("test_sorted_target_max_file_size", writerCount);
        getDataFiles("test_sorted_target_max_file_size").forEach(TestPrestoNativeIcebergWriterSessionProperties::assertSortedByPayload);
    }

    @Test
    public void testParquetWriterBlockSize()
    {
        assertRowGroupsFollowTarget("test_parquet_writer_block_size", UNSORTED);
    }

    @Test
    public void testParquetWriterBlockSizeOnSortedTable()
    {
        assertRowGroupsFollowTarget("test_sorted_parquet_writer_block_size", SORTED);
        getDataFiles("test_sorted_parquet_writer_block_size").forEach(TestPrestoNativeIcebergWriterSessionProperties::assertSortedByPayload);
    }

    @Test
    public void testParquetWriterPageSize()
    {
        // Without compression the decompressed page size is what the writer compares against the page size.
        Session uncompressed = withIcebergProperty(COMPRESSION_CODEC, "NONE");

        writeTable("test_default_page_size", uncompressed, UNSORTED);
        int largestDefaultPage = getDataFiles("test_default_page_size").stream()
                .flatMap(file -> getPayloadPages(file).stream())
                .flatMap(List::stream)
                .mapToInt(DataPage::getUncompressedSize)
                .max()
                .orElseThrow();
        // Without the property the pages exceed the bound checked below, so the bound proves the property is applied.
        assertTrue(largestDefaultPage > PAGE_TARGET_BYTES * 3 / 2, format("Default page of %s bytes is too small for this test", largestDefaultPage));

        writeTable("test_parquet_writer_page_size", Session.builder(uncompressed)
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_WRITER_PAGE_SIZE, "64kB")
                .build(), UNSORTED);
        for (String file : getDataFiles("test_parquet_writer_page_size")) {
            for (List<DataPage> pages : getPayloadPages(file)) {
                assertTrue(pages.size() > 1, format("Expected more than one page per column chunk in %s", file));
                for (int i = 0; i < pages.size(); i++) {
                    DataPage page = pages.get(i);
                    // The writer checks the page size after each batch of values, so a page can overshoot by a batch.
                    assertTrue(page.getUncompressedSize() <= PAGE_TARGET_BYTES * 2,
                            format("Page %s of %s has %s bytes, overshooting the target", i, file, page.getUncompressedSize()));
                    // The last page of a column chunk holds whatever is left. A dictionary encoded page holds
                    // dictionary indices, whose size does not follow the page size.
                    if (i < pages.size() - 1 && !isDictionaryEncoded(page)) {
                        assertTrue(page.getUncompressedSize() >= PAGE_TARGET_BYTES * 4 / 5,
                                format("Page %s of %s has %s bytes, closed before the target", i, file, page.getUncompressedSize()));
                    }
                }
            }
        }
    }

    @Test
    public void testNativeParquetWriterRowGroupMaxRows()
    {
        writeTable("test_default_row_group_rows", getSession(), UNSORTED);
        // The default cap of about 1M rows and the 128MB block size both exceed what one file holds here.
        for (String file : getDataFiles("test_default_row_group_rows")) {
            assertEquals(getRowGroups(file).size(), 1, format("Expected one row group in %s", file));
        }

        writeTable("test_parquet_writer_row_group_max_rows", withIcebergProperty(NATIVE_PARQUET_WRITER_ROW_GROUP_MAX_ROWS, "100000"), UNSORTED);
        for (String file : getDataFiles("test_parquet_writer_row_group_max_rows")) {
            List<BlockMetaData> rowGroups = getRowGroups(file);
            assertTrue(rowGroups.size() > 1, format("Expected more than one row group in %s", file));
            for (BlockMetaData rowGroup : rowGroups) {
                assertTrue(rowGroup.getRowCount() <= 100_000, format("Row group of %s rows in %s exceeds the cap", rowGroup.getRowCount(), file));
            }
        }
    }

    @Test
    public void testNativeParquetWriterPageMaxRows()
    {
        writeTable("test_default_page_max_rows", getSession(), UNSORTED);
        // By default pages close by size only, and 1MB of payload digests is far more than 20,000 rows.
        long largestDefaultPage = getDataFiles("test_default_page_max_rows").stream()
                .flatMap(file -> getPayloadPages(file).stream())
                .flatMap(List::stream)
                .mapToLong(DataPage::getValueCount)
                .max()
                .orElseThrow();
        assertTrue(largestDefaultPage > 20_000, format("Default page of %s rows is too small for this test", largestDefaultPage));

        writeTable("test_parquet_writer_page_max_rows", withIcebergProperty(NATIVE_PARQUET_WRITER_PAGE_MAX_ROWS, "20000"), UNSORTED);
        int checkedPages = 0;
        for (String file : getDataFiles("test_parquet_writer_page_max_rows")) {
            for (List<DataPage> pages : getPayloadPages(file)) {
                for (int i = 0; i < pages.size(); i++) {
                    DataPage page = pages.get(i);
                    assertTrue(page.getValueCount() <= 20_000, format("Page %s of %s has %s rows, exceeding the cap", i, file, page.getValueCount()));
                    // The last page of a column chunk holds what is left, and the page that the fallback from
                    // dictionary to plain encoding closes holds what was written before it. Every other page
                    // closes at the cap.
                    boolean last = i == pages.size() - 1;
                    boolean closedByFallback = !last && isDictionaryEncoded(page) && !isDictionaryEncoded(pages.get(i + 1));
                    if (!last && !closedByFallback) {
                        assertEquals(page.getValueCount(), 20_000, format("Page %s of %s closed before the cap", i, file));
                        checkedPages++;
                    }
                }
            }
        }
        assertTrue(checkedPages > 0, "No page was checked against the cap");
    }

    @Test
    public void testDefaultRowGroupMaxRowsCapsLargeBlockSize()
    {
        // Narrow rows reach the default cap of 1,048,576 rows well before a 160MB block size.
        Session session = Session.builder(largeWriteSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_WRITER_BLOCK_SIZE, LARGE_TARGET)
                .build();
        writeLargeTable("test_large_block_size_default_max_rows", session, UNSORTED);
        List<BlockMetaData> rowGroups = getDataFiles("test_large_block_size_default_max_rows").stream()
                .flatMap(file -> getRowGroups(file).stream())
                .collect(toImmutableList());
        assertTrue(rowGroups.stream().allMatch(rowGroup -> rowGroup.getRowCount() <= DEFAULT_ROW_GROUP_MAX_ROWS), format("Row groups exceed the default cap: %s", rowGroups));
        assertTrue(rowGroups.stream().anyMatch(rowGroup -> rowGroup.getRowCount() == DEFAULT_ROW_GROUP_MAX_ROWS), format("No row group reached the default cap: %s", rowGroups));
    }

    @Test
    public void testLargeBlockSizeAndPageSize()
    {
        assertLargeRowGroupsAndPagesFollowTargets("test_large_block_and_page_size", UNSORTED);
    }

    @Test
    public void testLargeBlockSizeAndPageSizeOnSortedTable()
    {
        assertLargeRowGroupsAndPagesFollowTargets("test_sorted_large_block_and_page_size", SORTED);
    }

    /// Writes 'table' with block size, file size and row cap set for one row group per file, as when each file
    /// should hold a single large row group, and checks the row group and page sizes.
    private void assertLargeRowGroupsAndPagesFollowTargets(String table, String tableProperties)
    {
        Session session = Session.builder(largeWriteSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_WRITER_BLOCK_SIZE, LARGE_TARGET)
                .setCatalogSessionProperty(ICEBERG_CATALOG, TARGET_MAX_FILE_SIZE, LARGE_TARGET)
                .setCatalogSessionProperty(ICEBERG_CATALOG, NATIVE_PARQUET_WRITER_ROW_GROUP_MAX_ROWS, "1000000000")
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_WRITER_PAGE_SIZE, "4MB")
                .build();
        writeLargeTable(table, session, tableProperties);

        List<String> files = getDataFiles(table);
        assertTrue(files.size() > 1, format("Expected more than one file, got %s", files));
        long smallRowGroups = 0;
        for (String file : files) {
            // A row group that the file size closes ends the file, so no small row group follows it.
            List<BlockMetaData> rowGroups = getRowGroups(file);
            assertEquals(rowGroups.size(), 1, format("Expected one row group in %s: %s", file, rowGroups));
            BlockMetaData rowGroup = rowGroups.get(0);
            assertTrue(rowGroup.getTotalByteSize() <= LARGE_TARGET_BYTES * 3 / 2, format("Row group of %s bytes in %s overshoots the target", rowGroup.getTotalByteSize(), file));
            // Only the last file holds what is left.
            if (rowGroup.getTotalByteSize() < LARGE_TARGET_BYTES * 4 / 5) {
                smallRowGroups++;
            }
            else {
                assertTrue(rowGroup.getRowCount() > DEFAULT_ROW_GROUP_MAX_ROWS, format("Row group of %s rows in %s is within the default cap", rowGroup.getRowCount(), file));
            }
            for (List<DataPage> pages : getPayloadPages(file)) {
                for (int i = 0; i < pages.size(); i++) {
                    DataPage page = pages.get(i);
                    assertTrue(page.getUncompressedSize() <= LARGE_PAGE_TARGET_BYTES * 2,
                            format("Page %s of %s has %s bytes, overshooting the target", i, file, page.getUncompressedSize()));
                    // The pages before the dictionary falls back to plain encoding hold dictionary indices.
                    if (i < pages.size() - 1 && !isDictionaryEncoded(page)) {
                        assertTrue(page.getUncompressedSize() >= LARGE_PAGE_TARGET_BYTES * 4 / 5,
                                format("Page %s of %s has %s bytes, closed before the target", i, file, page.getUncompressedSize()));
                    }
                }
            }
        }
        assertTrue(smallRowGroups <= 1, format("%s files are well below the target: %s", smallRowGroups, files));
        if (tableProperties.equals(SORTED)) {
            files.forEach(TestPrestoNativeIcebergWriterSessionProperties::assertSortedByPayload);
        }
    }

    private Session largeWriteSession()
    {
        // One writer, so that every file but the last is full. Without compression the decompressed sizes are
        // what the writer compares against the targets.
        return Session.builder(getSession())
                .setSystemProperty(TASK_WRITER_COUNT, "1")
                .setSystemProperty(SCALE_WRITERS, "false")
                .setCatalogSessionProperty(ICEBERG_CATALOG, COMPRESSION_CODEC, "NONE")
                .build();
    }

    private void writeLargeTable(String table, Session session, String tableProperties)
    {
        assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        assertUpdate(session, format("CREATE TABLE %s %s AS %s", table, tableProperties, LARGE_SOURCE_QUERY), LARGE_ROW_COUNT);
    }

    private void assertFileSizesFollowTarget(String table, int writerCount)
    {
        List<Long> fileSizes = getFileSizes(table);
        assertTrue(fileSizes.size() > writerCount, format("Expected more than %s files, got %s", writerCount, fileSizes.size()));
        for (long fileSize : fileSizes) {
            assertTrue(fileSize <= TARGET_BYTES * 3 / 2, format("File of %s bytes overshoots the target: %s", fileSize, fileSizes));
        }
        // Only the last file of each writer may be closed before it reaches the target.
        long smallFiles = fileSizes.stream().filter(fileSize -> fileSize < TARGET_BYTES * 9 / 10).count();
        assertTrue(smallFiles <= writerCount, format("%s files are well below the target with %s writers: %s", smallFiles, writerCount, fileSizes));
    }

    /// Writes 'table' with the block size set and checks the sizes of its row groups.
    private void assertRowGroupsFollowTarget(String table, String tableProperties)
    {
        // The writer compares the block size against compressed pages plus the uncompressed current page, so
        // only without compression does the decompressed row group size track the block size.
        Session uncompressed = withIcebergProperty(COMPRESSION_CODEC, "NONE");

        writeTable(table + "_default", uncompressed, tableProperties);
        long largestDefaultRowGroup = getDataFiles(table + "_default").stream()
                .flatMap(file -> getRowGroups(file).stream())
                .mapToLong(BlockMetaData::getTotalByteSize)
                .max()
                .orElseThrow();
        // Without the property the row groups exceed the bound checked below, so the bound proves the property is applied.
        assertTrue(largestDefaultRowGroup > TARGET_BYTES * 3 / 2, format("Default row group of %s bytes is too small for this test", largestDefaultRowGroup));

        Session session = Session.builder(uncompressed)
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_WRITER_BLOCK_SIZE, TARGET)
                .build();
        writeTable(table, session, tableProperties);
        for (String file : getDataFiles(table)) {
            List<BlockMetaData> rowGroups = getRowGroups(file);
            assertTrue(rowGroups.size() > 1, format("Expected more than one row group in %s", file));
            for (int i = 0; i < rowGroups.size(); i++) {
                long rowGroupBytes = rowGroups.get(i).getTotalByteSize();
                assertTrue(rowGroupBytes <= TARGET_BYTES * 3 / 2,
                        format("Row group %s of %s has %s bytes, overshooting the target", i, file, rowGroupBytes));
                // The last row group holds whatever is left, so only the earlier ones must reach the target. The
                // writer counts dictionary bytes that it drops when it falls back to plain encoding, so a row group
                // can close slightly below the target.
                if (i < rowGroups.size() - 1) {
                    assertTrue(rowGroupBytes >= TARGET_BYTES * 4 / 5,
                            format("Row group %s of %s has %s bytes, closed before the target", i, file, rowGroupBytes));
                }
            }
        }
    }

    private Session withIcebergProperty(String name, String value)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, name, value)
                .build();
    }

    private void writeTable(String table, Session session, String tableProperties)
    {
        assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        assertUpdate(session, format("CREATE TABLE %s %s AS %s", table, tableProperties, SOURCE_QUERY), ROW_COUNT);
        assertEquals(computeActual(format("SELECT count(*) FROM %s", table)).getOnlyValue(), ROW_COUNT);
    }

    private List<Long> getFileSizes(String table)
    {
        return computeActual(format("SELECT file_size_in_bytes FROM \"%s$files\"", table)).getMaterializedRows().stream()
                .map(row -> (Long) row.getField(0))
                .collect(toImmutableList());
    }

    private List<String> getDataFiles(String table)
    {
        return computeActual(format("SELECT file_path FROM \"%s$files\"", table)).getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
    }

    private static void assertSortedByPayload(String file)
    {
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file))
                .withConf(new Configuration())
                .build()) {
            String previous = null;
            for (Group row = reader.read(); row != null; row = reader.read()) {
                String payload = row.getString("payload", 0);
                assertTrue(previous == null || previous.compareTo(payload) <= 0, format("Rows of %s are not sorted by payload", file));
                previous = payload;
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /// Returns the data pages of the payload column, one list per row group.
    private static List<List<DataPage>> getPayloadPages(String file)
    {
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file), new Configuration()))) {
            ColumnDescriptor payload = reader.getFileMetaData().getSchema().getColumnDescription(new String[] {"payload"});
            ImmutableList.Builder<List<DataPage>> pagesPerRowGroup = ImmutableList.builder();
            for (PageReadStore rowGroup = reader.readNextRowGroup(); rowGroup != null; rowGroup = reader.readNextRowGroup()) {
                PageReader pageReader = rowGroup.getPageReader(payload);
                ImmutableList.Builder<DataPage> pages = ImmutableList.builder();
                for (DataPage page = pageReader.readPage(); page != null; page = pageReader.readPage()) {
                    pages.add(page);
                }
                pagesPerRowGroup.add(pages.build());
            }
            return pagesPerRowGroup.build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isDictionaryEncoded(DataPage page)
    {
        return page instanceof DataPageV1 && ((DataPageV1) page).getValueEncoding().usesDictionary();
    }

    private static List<BlockMetaData> getRowGroups(String file)
    {
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file), new Configuration()))) {
            return reader.getRowGroups();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
