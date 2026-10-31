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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
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
    private static final String COMPRESSION_CODEC = "compression_codec";
    private static final long TARGET_BYTES = 1L << 20;
    private static final String TARGET = "1MB";
    private static final long ROW_COUNT = 500_000;
    // Hex digests do not compress well, so the data stays around 10MB once written.
    private static final String SOURCE_QUERY = "SELECT x * 1000 + y AS id, " +
            "to_hex(md5(to_utf8(CAST(x * 1000 + y AS VARCHAR)))) AS payload " +
            "FROM UNNEST(sequence(0, 499)) t(x) CROSS JOIN UNNEST(sequence(0, 999)) u(y)";

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
        writeTable("test_default_file_size", getSession());
        // Each writer produces one file by default.
        int writerCount = getFileSizes("test_default_file_size").size();

        writeTable("test_target_max_file_size", withIcebergProperty(TARGET_MAX_FILE_SIZE, TARGET));
        List<Long> fileSizes = getFileSizes("test_target_max_file_size");

        assertTrue(fileSizes.size() > writerCount, format("Expected more than %s files, got %s", writerCount, fileSizes.size()));
        for (long fileSize : fileSizes) {
            assertTrue(fileSize <= TARGET_BYTES * 3 / 2, format("File of %s bytes overshoots the target: %s", fileSize, fileSizes));
        }
        // Only the last file of each writer may be closed before it reaches the target.
        long smallFiles = fileSizes.stream().filter(fileSize -> fileSize < TARGET_BYTES * 9 / 10).count();
        assertTrue(smallFiles <= writerCount, format("%s files are well below the target with %s writers: %s", smallFiles, writerCount, fileSizes));
    }

    @Test
    public void testParquetWriterBlockSize()
    {
        // The writer compares the block size against compressed pages plus the uncompressed current page, so
        // only without compression does the decompressed row group size track the block size.
        Session uncompressed = withIcebergProperty(COMPRESSION_CODEC, "NONE");

        writeTable("test_default_block_size", uncompressed);
        long largestDefaultRowGroup = getDataFiles("test_default_block_size").stream()
                .flatMap(file -> getRowGroups(file).stream())
                .mapToLong(BlockMetaData::getTotalByteSize)
                .max()
                .orElseThrow();
        // Without the property the row groups exceed the bound checked below, so the bound proves the property is applied.
        assertTrue(largestDefaultRowGroup > TARGET_BYTES * 3 / 2, format("Default row group of %s bytes is too small for this test", largestDefaultRowGroup));

        Session session = Session.builder(uncompressed)
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_WRITER_BLOCK_SIZE, TARGET)
                .build();
        writeTable("test_parquet_writer_block_size", session);
        for (String file : getDataFiles("test_parquet_writer_block_size")) {
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

    private void writeTable(String table, Session session)
    {
        assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        assertUpdate(session, format("CREATE TABLE %s AS %s", table, SOURCE_QUERY), ROW_COUNT);
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
