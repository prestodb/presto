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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.TASK_WRITER_COUNT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestIcebergTargetMaxFileSize
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(CatalogType.HIVE)
                .setExtraConnectorProperties(ImmutableMap.of(
                        "iceberg.target-max-file-size", "40kB"))
                .build().getQueryRunner();
    }

    @Test
    public void testTargetMaxFileSize()
    {
        String tableName = "test_target_max_file_size_" + System.currentTimeMillis();

        try {
            // Create table with small target file size (40kB configured in connector properties)
            Session session = Session.builder(getSession())
                    .setSystemProperty(TASK_WRITER_COUNT, "1")
                    .build();

            assertUpdate(session,
                    String.format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.lineitem", tableName),
                    60175);

            // Verify data is correct
            MaterializedResult countResult = computeActual(
                    String.format("SELECT count(*) FROM %s", tableName));
            assertEquals(countResult.getOnlyValue(), 60175L);

            // Check number of files created
            MaterializedResult fileCountResult = computeActual(
                    String.format("SELECT count(*) FROM \"%s$files\"", tableName));
            long fileCount = (Long) fileCountResult.getOnlyValue();

            // With 40kB limit, we should create significantly more files than default
            // tpch.tiny.lineitem is about 2.4MB, so we expect at least 10 files
            assertTrue(fileCount > 10,
                    String.format("Expected > 10 files with 40kB limit, got %d", fileCount));

            // Verify file sizes are reasonable (not much larger than target)
            MaterializedResult fileSizes = computeActual(
                    String.format("SELECT file_size_in_bytes FROM \"%s$files\"", tableName));

            long targetSize = 40 * 1024; // 40kB
            long maxAllowedSize = targetSize * 10; // Allow up to 10x the target size
            for (MaterializedRow row : fileSizes.getMaterializedRows()) {
                long fileSize = (Long) row.getField(0);
                assertTrue(fileSize > 0 && fileSize <= maxAllowedSize,
                        String.format("File size %d bytes is outside expected range (1 to %d bytes)",
                                fileSize, maxAllowedSize));
            }
        }
        finally {
            // Cleanup
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testTargetMaxFileSizeWithPartitioning()
    {
        String tableName = "test_target_max_file_size_partitioned_" + System.currentTimeMillis();

        try {
            // Create partitioned table with small file size limit (40kB configured in connector properties)
            Session session = Session.builder(getSession())
                    .setSystemProperty(TASK_WRITER_COUNT, "1")
                    .build();

            assertUpdate(session,
                    String.format("CREATE TABLE %s (orderkey BIGINT, partkey BIGINT, suppkey BIGINT, " +
                            "linenumber INTEGER, quantity DOUBLE, extendedprice DOUBLE, discount DOUBLE, " +
                            "tax DOUBLE, returnflag VARCHAR, linestatus VARCHAR, shipdate DATE, " +
                            "commitdate DATE, receiptdate DATE, shipinstruct VARCHAR, shipmode VARCHAR, " +
                            "comment VARCHAR) WITH (partitioning = ARRAY['returnflag'])", tableName));

            assertUpdate(session,
                    String.format("INSERT INTO %s SELECT * FROM tpch.tiny.lineitem", tableName),
                    60175);

            // Verify data
            MaterializedResult countResult = computeActual(
                    String.format("SELECT count(*) FROM %s", tableName));
            assertEquals(countResult.getOnlyValue(), 60175L);

            // Check total number of files
            MaterializedResult totalFilesResult = computeActual(
                    String.format("SELECT count(*) FROM \"%s$files\"", tableName));
            long totalFiles = (Long) totalFilesResult.getOnlyValue();

            // Should have multiple files across partitions
            // With partitioning and 40kB limit, we expect even more files than the non-partitioned case
            assertTrue(totalFiles > 10,
                    String.format("Expected > 10 total files with 40kB limit and partitioning, got %d", totalFiles));

            // Verify file sizes are reasonable
            MaterializedResult fileSizes = computeActual(
                    String.format("SELECT file_size_in_bytes FROM \"%s$files\"", tableName));

            long targetSize = 40 * 1024; // 40kB
            long maxAllowedSize = targetSize * 10; // Allow up to 10x the target size
            for (MaterializedRow row : fileSizes.getMaterializedRows()) {
                long fileSize = (Long) row.getField(0);
                assertTrue(fileSize > 0 && fileSize <= maxAllowedSize,
                        String.format("File size %d bytes is outside expected range (1 to %d bytes)",
                                fileSize, maxAllowedSize));
            }
        }
        finally {
            // Cleanup
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }
}
