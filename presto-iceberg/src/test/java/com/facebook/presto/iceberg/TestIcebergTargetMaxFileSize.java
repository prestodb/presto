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

import java.util.OptionalInt;

import static com.facebook.presto.SystemSessionProperties.TASK_WRITER_COUNT;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
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
        // Note: This test uses estimates (> 10 files) to test the default multi-worker scenario
        // without modifying worker count, split sizes, or other parameters that would make
        // the file count deterministic. The 40kB target file size should create significantly
        // more files than the default 1GB, demonstrating that the configuration is working.
        String tableName = "test_target_max_file_size_" + randomTableSuffix();

        try {
            // Create table with small target file size (40kB configured in connector properties)
            assertUpdate(String.format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.lineitem", tableName),
                    60175);

            MaterializedResult countResult = computeActual(
                    String.format("SELECT count(*) FROM %s", tableName));
            assertEquals(countResult.getOnlyValue(), 60175L);

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

            long targetSize = 40 * 1024;
            long maxAllowedSize = targetSize * 10;
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
        // Note: This test uses estimates (> 10 files) to test the default multi-worker scenario
        // with partitioning, without modifying worker count, split sizes, or other parameters
        // that would make the file count deterministic. The 40kB target file size should create
        // significantly more files than the default 1GB, demonstrating that the configuration
        // is working across partitions.
        String tableName = "test_target_max_file_size_partitioned_" + randomTableSuffix();

        try {
            // Create partitioned table with small file size limit (40kB configured in connector properties)
            assertUpdate(String.format("CREATE TABLE %s (orderkey BIGINT, partkey BIGINT, suppkey BIGINT, " +
                            "linenumber INTEGER, quantity DOUBLE, extendedprice DOUBLE, discount DOUBLE, " +
                            "tax DOUBLE, returnflag VARCHAR, linestatus VARCHAR, shipdate DATE, " +
                            "commitdate DATE, receiptdate DATE, shipinstruct VARCHAR, shipmode VARCHAR, " +
                            "comment VARCHAR) WITH (partitioning = ARRAY['returnflag'])", tableName));

            assertUpdate(String.format("INSERT INTO %s SELECT * FROM tpch.tiny.lineitem", tableName),
                    60175);

            MaterializedResult countResult = computeActual(
                    String.format("SELECT count(*) FROM %s", tableName));
            assertEquals(countResult.getOnlyValue(), 60175L);

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
            long maxAllowedSize = targetSize * 10;
            for (MaterializedRow row : fileSizes.getMaterializedRows()) {
                long fileSize = (Long) row.getField(0);
                assertTrue(fileSize > 0 && fileSize <= maxAllowedSize,
                        String.format("File size %d bytes is outside expected range (1 to %d bytes)",
                                fileSize, maxAllowedSize));
            }
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testDefaultTargetMaxFileSizeUnpartitioned()
            throws Exception
    {
        // Create a query runner with default 1GB target size and single worker
        QueryRunner defaultQueryRunner = IcebergQueryRunner.builder()
                .setCatalogType(CatalogType.HIVE)
                .setNodeCount(OptionalInt.of(1))
                .build().getQueryRunner();

        try {
            String tableName = "test_default_unpartitioned_" + randomTableSuffix();

            try {
                // With default 1GB target size, TASK_WRITER_COUNT=1, and single worker
                Session session = Session.builder(defaultQueryRunner.getDefaultSession())
                        .setSystemProperty(TASK_WRITER_COUNT, "1")
                        .build();

                defaultQueryRunner.execute(session,
                        String.format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.lineitem", tableName));

                MaterializedResult countResult = defaultQueryRunner.execute(
                        String.format("SELECT count(*) FROM %s", tableName));
                assertEquals(countResult.getOnlyValue(), 60175L);

                MaterializedResult fileCountResult = defaultQueryRunner.execute(
                        String.format("SELECT count(*) FROM \"%s$files\"", tableName));
                long fileCount = (Long) fileCountResult.getOnlyValue();

                // With default 1GB limit, TASK_WRITER_COUNT=1, and single worker,
                // tpch.tiny.lineitem (~1.68MB) should create exactly 1 file
                assertEquals(fileCount, 1L,
                        String.format("Expected exactly 1 file with default 1GB limit, single worker, and TASK_WRITER_COUNT=1, got %d", fileCount));
            }
            finally {
                defaultQueryRunner.execute(String.format("DROP TABLE IF EXISTS %s", tableName));
            }
        }
        finally {
            defaultQueryRunner.close();
        }
    }

    @Test
    public void testDefaultTargetMaxFileSizePartitioned()
            throws Exception
    {
        // Create a query runner with default 1GB target size and single worker
        QueryRunner defaultQueryRunner = IcebergQueryRunner.builder()
                .setCatalogType(CatalogType.HIVE)
                .setNodeCount(OptionalInt.of(1))
                .build().getQueryRunner();

        try {
            String tableName = "test_default_partitioned_" + randomTableSuffix();

            try {
                // With default 1GB target size, TASK_WRITER_COUNT=1, and single worker
                Session session = Session.builder(defaultQueryRunner.getDefaultSession())
                        .setSystemProperty(TASK_WRITER_COUNT, "1")
                        .build();

                defaultQueryRunner.execute(session,
                        String.format("CREATE TABLE %s (orderkey BIGINT, partkey BIGINT, suppkey BIGINT, " +
                                "linenumber INTEGER, quantity DOUBLE, extendedprice DOUBLE, discount DOUBLE, " +
                                "tax DOUBLE, returnflag VARCHAR, linestatus VARCHAR, shipdate DATE, " +
                                "commitdate DATE, receiptdate DATE, shipinstruct VARCHAR, shipmode VARCHAR, " +
                                "comment VARCHAR) WITH (partitioning = ARRAY['returnflag'])", tableName));

                defaultQueryRunner.execute(session,
                        String.format("INSERT INTO %s SELECT * FROM tpch.tiny.lineitem", tableName));

                MaterializedResult countResult = defaultQueryRunner.execute(
                        String.format("SELECT count(*) FROM %s", tableName));
                assertEquals(countResult.getOnlyValue(), 60175L);

                MaterializedResult partitionCountResult = defaultQueryRunner.execute(
                        String.format("SELECT count(DISTINCT returnflag) FROM %s", tableName));
                long partitionCount = (Long) partitionCountResult.getOnlyValue();

                MaterializedResult totalFilesResult = defaultQueryRunner.execute(
                        String.format("SELECT count(*) FROM \"%s$files\"", tableName));
                long totalFiles = (Long) totalFilesResult.getOnlyValue();

                // With default 1GB limit, TASK_WRITER_COUNT=1, and single worker,
                // we should have exactly 1 file per partition (tpch.tiny.lineitem has 3 distinct returnflag values: A, N, R)
                assertEquals(totalFiles, partitionCount,
                        String.format("Expected exactly %d files (1 per partition) with default 1GB limit, single worker, and TASK_WRITER_COUNT=1, got %d",
                                partitionCount, totalFiles));
            }
            finally {
                defaultQueryRunner.execute(String.format("DROP TABLE IF EXISTS %s", tableName));
            }
        }
        finally {
            defaultQueryRunner.close();
        }
    }

    @Test
    public void testSessionPropertyOverridesDefault()
    {
        String tableName = "test_session_overrides_default_" + randomTableSuffix();

        try {
            // Use smaller page/block sizes with 900kB target file size for deterministic file splitting
            // This overrides the default 40kB config property
            Session session = Session.builder(getSession())
                    .setSystemProperty(TASK_WRITER_COUNT, "1")
                    .setCatalogSessionProperty("iceberg", "parquet_writer_block_size", "2kB")
                    .setCatalogSessionProperty("iceberg", "parquet_writer_page_size", "1kB")
                    .setCatalogSessionProperty("iceberg", "target_max_file_size", "900kB")
                    .build();

            assertUpdate(session,
                    String.format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.lineitem", tableName),
                    60175);

            MaterializedResult countResult = computeActual(session,
                    String.format("SELECT count(*) FROM %s", tableName));
            assertEquals(countResult.getOnlyValue(), 60175L);

            MaterializedResult fileCountResult = computeActual(session,
                    String.format("SELECT count(*) FROM \"%s$files\"", tableName));
            long fileCount = (Long) fileCountResult.getOnlyValue();

            // With 900kB session property (overriding 40kB config),
            // tpch.tiny.lineitem (~1.68MB) should create exactly 2 files
            assertEquals(fileCount, 2L,
                    String.format("Expected exactly 2 files with 900kB session property, got %d", fileCount));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSessionPropertyWithoutConfigProperty()
            throws Exception
    {
        // Create a query runner with NO config property (default 1GB)
        QueryRunner defaultQueryRunner = IcebergQueryRunner.builder()
                .setCatalogType(CatalogType.HIVE)
                .setNodeCount(OptionalInt.of(1))
                .build().getQueryRunner();

        try {
            String tableName = "test_session_no_config_" + randomTableSuffix();

            try {
                // Use smaller page/block sizes with 900kB target file size for deterministic file splitting
                // This overrides the default 1GB config
                Session session = Session.builder(defaultQueryRunner.getDefaultSession())
                        .setSystemProperty(TASK_WRITER_COUNT, "1")
                        .setCatalogSessionProperty("iceberg", "parquet_writer_block_size", "2kB")
                        .setCatalogSessionProperty("iceberg", "parquet_writer_page_size", "1kB")
                        .setCatalogSessionProperty("iceberg", "target_max_file_size", "900kB")
                        .build();

                defaultQueryRunner.execute(session,
                        String.format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.lineitem", tableName));

                MaterializedResult countResult = defaultQueryRunner.execute(
                        String.format("SELECT count(*) FROM %s", tableName));
                assertEquals(countResult.getOnlyValue(), 60175L);

                MaterializedResult fileCountResult = defaultQueryRunner.execute(
                        String.format("SELECT count(*) FROM \"%s$files\"", tableName));
                long fileCount = (Long) fileCountResult.getOnlyValue();

                // With 900kB session property (overriding default 1GB),
                // tpch.tiny.lineitem (~1.68MB) should create exactly 2 files
                assertEquals(fileCount, 2L,
                        String.format("Expected exactly 2 files with 900kB session property, got %d", fileCount));
            }
            finally {
                defaultQueryRunner.execute(String.format("DROP TABLE IF EXISTS %s", tableName));
            }
        }
        finally {
            defaultQueryRunner.close();
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Could not coerce value '-1B' to DataSize.*iceberg.target-max-file-size.*")
    public void testNegativeConfigProperty()
            throws Exception
    {
        // Create a query runner with negative config property value
        // This should fail during initialization because DataSize.valueOf() rejects negative values
        QueryRunner negativeConfigRunner = IcebergQueryRunner.builder()
                .setCatalogType(CatalogType.HIVE)
                .setExtraConnectorProperties(ImmutableMap.of(
                        "iceberg.target-max-file-size", "-1B"))
                .build().getQueryRunner();

        negativeConfigRunner.close();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*iceberg.target-max-file-size must be at least 1 byte.*")
    public void testZeroConfigProperty()
            throws Exception
    {
        // Create a query runner with zero config property value
        // This should fail during initialization due to custom validation in setter
        QueryRunner zeroConfigRunner = IcebergQueryRunner.builder()
                .setCatalogType(CatalogType.HIVE)
                .setExtraConnectorProperties(ImmutableMap.of(
                        "iceberg.target-max-file-size", "0B"))
                .build().getQueryRunner();

        zeroConfigRunner.close();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*iceberg.target_max_file_size is invalid: -1B.*")
    public void testNegativeSessionProperty()
    {
        String tableName = "test_negative_session_" + randomTableSuffix();

        try {
            // Try to set negative session property value
            Session session = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", "target_max_file_size", "-1B")
                    .build();

            // This should fail when the session property is validated
            assertUpdate(session,
                    String.format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.lineitem", tableName),
                    60175);
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Invalid value for target_max_file_size.*must be at least 1 byte.*")
    public void testZeroSessionProperty()
    {
        String tableName = "test_zero_session_" + randomTableSuffix();

        try {
            // Try to set zero session property value
            Session session = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", "target_max_file_size", "0B")
                    .build();

            // This should fail when the session property is validated
            assertUpdate(session,
                    String.format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.lineitem", tableName),
                    60175);
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }
}
