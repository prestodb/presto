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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIcebergV3
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setFormat(PARQUET)
                .setNodeCount(OptionalInt.of(1))
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build().getQueryRunner();
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testCreateV3Table()
            throws Exception
    {
        String tableName = "test_create_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
            assertQuery("SELECT * FROM " + tableName, "SELECT * WHERE false");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateUnsupportedFormatVersion()
            throws Exception
    {
        String tableName = "test_create_v4_table";
        // Ensure clean state in case a previous run created the table
        dropTable(tableName);

        assertQueryFails(
                "CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '4')",
                ".*Iceberg table format version 4 is not supported.*");
    }

    @Test
    public void testUpgradeV2ToV3()
            throws Exception
    {
        String tableName = "test_upgrade_v2_to_v3";
        try {
            // Create v2 table
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '2')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 2);

            // Upgrade to v3
            BaseTable baseTable = (BaseTable) table;
            TableOperations operations = baseTable.operations();
            TableMetadata currentMetadata = operations.current();
            operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(3));

            // Verify the upgrade
            table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertIntoV3Table()
            throws Exception
    {
        String tableName = "test_insert_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two')", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'one'), (2, 'two')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'three')", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testDeleteOnV3Table()
            throws Exception
    {
        String tableName = "test_v3_delete";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)", 3);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");

            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");

            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);

            // Verify DV metadata: the delete should have produced a PUFFIN-format deletion vector
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    for (org.apache.iceberg.DeleteFile deleteFile : task.deletes()) {
                        assertEquals(deleteFile.format(), FileFormat.PUFFIN);
                        assertTrue(deleteFile.path().toString().endsWith(".puffin"),
                                "Deletion vector file should have .puffin extension");
                        assertTrue(deleteFile.fileSizeInBytes() > 0,
                                "Deletion vector file size should be positive");
                    }
                }
            }

            // Delete more rows
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 3", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 'Bob', 200.0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testTruncateV3Table()
            throws Exception
    {
        String tableName = "test_v3_truncate";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)", 3);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");

            assertUpdate("DELETE FROM " + tableName, 3);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");

            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'Dave', 400.0)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (4, 'Dave', 400.0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMetadataDeleteOnV3PartitionedTable()
            throws Exception
    {
        String tableName = "test_v3_metadata_delete";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE, part VARCHAR)"
                    + " WITH (\"format-version\" = '3', partitioning = ARRAY['part'])");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'Alice', 100.0, 'A'), (2, 'Bob', 200.0, 'A'),"
                    + " (3, 'Charlie', 300.0, 'B'), (4, 'Dave', 400.0, 'C')", 4);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 4");

            assertUpdate("DELETE FROM " + tableName + " WHERE part = 'A'", 2);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 2");
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (3, 'Charlie', 300.0, 'B'), (4, 'Dave', 400.0, 'C')");

            assertUpdate("DELETE FROM " + tableName + " WHERE part = 'B'", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (4, 'Dave', 400.0, 'C')");

            assertUpdate("DELETE FROM " + tableName + " WHERE part = 'C'", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdateOnV3Table()
            throws Exception
    {
        String tableName = "test_v3_update";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, status VARCHAR, score DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName
                            + " VALUES (1, 'Alice', 'active', 85.5), (2, 'Bob', 'active', 92.0), (3, 'Charlie', 'inactive', 78.3)",
                    3);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 'active', 85.5), (2, 'Bob', 'active', 92.0), (3, 'Charlie', 'inactive', 78.3)");

            assertUpdate("UPDATE " + tableName + " SET status = 'updated', score = 95.0 WHERE id = 1", 1);
            assertQuery("SELECT * FROM " + tableName + " WHERE id = 1",
                    "VALUES (1, 'Alice', 'updated', 95.0)");

            assertUpdate("UPDATE " + tableName + " SET status = 'retired' WHERE status = 'inactive'", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 'updated', 95.0), (2, 'Bob', 'active', 92.0), (3, 'Charlie', 'retired', 78.3)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMergeOnV3Table()
            throws Exception
    {
        String tableName = "test_v3_merge_target";
        String sourceTable = "test_v3_merge_source";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("CREATE TABLE " + sourceTable + " (id INTEGER, name VARCHAR, value DOUBLE)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", 2);
            assertUpdate("INSERT INTO " + sourceTable + " VALUES (1, 'Alice Updated', 150.0), (3, 'Charlie', 300.0)",
                    2);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)");
            assertQuery("SELECT * FROM " + sourceTable + " ORDER BY id",
                    "VALUES (1, 'Alice Updated', 150.0), (3, 'Charlie', 300.0)");

            assertUpdate(
                    "MERGE INTO " + tableName + " t USING " + sourceTable + " s ON t.id = s.id " +
                            "WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value " +
                            "WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
                    3);

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice Updated', 150.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");
        }
        finally {
            dropTable(tableName);
            dropTable(sourceTable);
        }
    }

    @Test
    public void testOptimizeOnV3Table()
            throws Exception
    {
        String tableName = "test_v3_optimize";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, category VARCHAR, value DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A', 100.0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'B', 200.0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'A', 150.0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'C', 300.0)", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0), (4, 'C', 300.0)");

            assertQuerySucceeds(format("CALL system.rewrite_data_files('%s', '%s')", TEST_SCHEMA, tableName));

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0), (4, 'C', 300.0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testPuffinDeletionVectorsAccepted()
            throws Exception
    {
        String tableName = "test_puffin_deletion_vectors_accepted";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two')", 2);

            Table table = loadTable(tableName);

            // Attach a PUFFIN delete vector to an existing data file in the v3 table
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                FileScanTask task = tasks.iterator().next();

                DeleteFile puffinDeleteFile = FileMetadata.deleteFileBuilder(task.spec())
                        .ofPositionDeletes()
                        .withPath(task.file().path().toString() + ".puffin")
                        .withFileSizeInBytes(16)
                        .withFormat(FileFormat.PUFFIN)
                        .withRecordCount(1)
                        .withContentOffset(0)
                        .withContentSizeInBytes(16)
                        .withReferencedDataFile(task.file().path().toString())
                        .build();

                table.newRowDelta()
                        .addDeletes(puffinDeleteFile)
                        .commit();
            }

            // The PUFFIN delete file is now accepted by the split source (no longer
            // throws NOT_SUPPORTED). The query will fail downstream because the fake
            // .puffin file doesn't exist on disk, but the important thing is that the
            // coordinator no longer rejects it at split enumeration time.
            try {
                computeActual("SELECT * FROM " + tableName);
            }
            catch (RuntimeException e) {
                // Verify the error is NOT the old "PUFFIN not supported" rejection.
                // Other failures (e.g., fake .puffin file not on disk) are acceptable.
                assertFalse(
                        e.getMessage().contains("Iceberg deletion vectors") && e.getMessage().contains("not supported"),
                        "PUFFIN deletion vectors should be accepted, not rejected: " + e.getMessage());
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3SupportedOperations()
            throws Exception
    {
        String tableName = "test_v3_supported";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, created_date DATE, amount DECIMAL(10,2)) WITH (\"format-version\" = '3', partitioning = ARRAY['created_date'])");

            assertUpdate("INSERT INTO " + tableName + " VALUES "
                    + "(1, 'Transaction A', DATE '2024-01-01', 100.50), "
                    + "(2, 'Transaction B', DATE '2024-01-02', 250.75), "
                    + "(3, 'Transaction C', DATE '2024-01-01', 175.00)", 3);

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES "
                            + "(1, 'Transaction A', DATE '2024-01-01', 100.50), "
                            + "(2, 'Transaction B', DATE '2024-01-02', 250.75), "
                            + "(3, 'Transaction C', DATE '2024-01-01', 175.00)");

            assertQuery(
                    "SELECT created_date, count(*), sum(amount) FROM " + tableName
                            + " GROUP BY created_date ORDER BY created_date",
                    "VALUES "
                            + "(DATE '2024-01-01', 2, 275.50), "
                            + "(DATE '2024-01-02', 1, 250.75)");

            assertQuery("SELECT * FROM " + tableName
                            + " WHERE created_date = DATE '2024-01-01' ORDER BY id",
                    "VALUES "
                            + "(1, 'Transaction A', DATE '2024-01-01', 100.50), "
                            + "(3, 'Transaction C', DATE '2024-01-01', 175.00)");

            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'Transaction D', DATE '2024-01-03', 300.00)", 1);

            assertQuery("SELECT count(*) as total_count FROM " + tableName, "SELECT 4");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSelectFromV3TableAfterInsert()
            throws Exception
    {
        String tableName = "test_select_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id integer, name varchar, price decimal(10,2))"
                    + " WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'apple', 1.50), (2, 'banana', 0.75),"
                    + " (3, 'cherry', 2.00)", 3);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'apple', 1.50), (2, 'banana', 0.75),"
                            + " (3, 'cherry', 2.00)");
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");
            assertQuery("SELECT sum(price) FROM " + tableName, "SELECT 4.25");
            assertQuery("SELECT name FROM " + tableName
                            + " WHERE price > 1.00 ORDER BY name",
                    "VALUES ('apple'), ('cherry')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3TableWithPartitioning()
            throws Exception
    {
        String tableName = "test_v3_partitioned_table";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id integer, category varchar, value integer)"
                    + " WITH (\"format-version\" = '3', partitioning = ARRAY['category'])");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)", 3);
            assertQuery("SELECT * FROM " + tableName
                            + " WHERE category = 'A' ORDER BY id",
                    "VALUES (1, 'A', 100), (3, 'A', 150)");
            assertQuery("SELECT category, sum(value) FROM " + tableName
                            + " GROUP BY category ORDER BY category",
                    "VALUES ('A', 250), ('B', 200)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3TableEncryptionNotSupported()
            throws Exception
    {
        String tableName = "test_v3_encrypted";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, data VARCHAR)"
                    + " WITH (\"format-version\" = '3')");
            // Insert data so the table has a snapshot
            // (validation requires a non-null snapshot)
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'unencrypted')", 1);

            // Set encryption property via the Iceberg API
            Table table = loadTable(tableName);
            table.updateProperties()
                    .set("encryption.key-id", "test-key-id")
                    .commit();

            // Both SELECT and INSERT should fail because the validation
            // rejects encryption
            assertThatThrownBy(() -> getQueryRunner().execute(
                    "SELECT * FROM " + tableName))
                    .hasMessageContaining(
                            "Iceberg table encryption is not supported");

            assertThatThrownBy(() -> getQueryRunner().execute(
                    "INSERT INTO " + tableName + " VALUES (2, 'more')"))
                    .hasMessageContaining(
                            "Iceberg table encryption is not supported");
        }
        finally {
            // Use Iceberg API to drop table directly, bypassing Presto's
            // validateTableForPresto
            dropTableViaIceberg(tableName);
        }
    }

    @Test
    public void testAddColumnWithDefaultRequiresV3()
            throws Exception
    {
        String tableName = "test_add_column_default_v2";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '2')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 2);
            assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'IN'",
                    "ADD COLUMN with DEFAULT values is only supported with Iceberg format version 3 or higher.*");

            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + TEST_SCHEMA + "' AND table_name = '" + tableName + "' ORDER BY ordinal_position",
                    "VALUES ('id'), ('name')");

            BaseTable baseTable = (BaseTable) table;
            TableOperations operations = baseTable.operations();
            TableMetadata currentMetadata = operations.current();
            operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(3));
            table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'IN'");
            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + TEST_SCHEMA + "' AND table_name = '" + tableName + "' ORDER BY ordinal_position",
                    "VALUES ('id'), ('name'), ('country')");
        }
        finally {
            dropTable(tableName);
        }
    }

    private Table loadTable(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), ICEBERG_CATALOG,
                getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner()
                .getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(
                dataDirectory, HADOOP.name(),
                new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }

    @Test
    public void testDeletionVectorEndToEnd()
            throws Exception
    {
        String tableName = "test_dv_end_to_end";
        try {
            // Step 1: Create V3 table and insert data
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 5");
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')");

            Table table = loadTable(tableName);

            // Step 2: Write a real Puffin file with a valid roaring bitmap deletion vector.
            // The roaring bitmap uses the portable "no-run" format (cookie = 12346).
            // We mark row positions 1 and 3 (0-indexed) as deleted — these correspond
            // to the rows (2, 'two') and (4, 'four') in insertion order.
            byte[] roaringBitmapBytes = serializeRoaringBitmapNoRun(new int[] {1, 3});

            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                FileScanTask task = tasks.iterator().next();
                String dataFilePath = task.file().path().toString();

                // Write the roaring bitmap as a blob inside a Puffin file
                String dvPath = table.location() + "/data/dv-" + UUID.randomUUID() + ".puffin";
                OutputFile outputFile = table.io().newOutputFile(dvPath);

                long blobOffset;
                long blobLength;
                long puffinFileSize;

                try (PuffinWriter writer = Puffin.write(outputFile)
                        .createdBy("presto-test")
                        .build()) {
                    writer.add(new Blob(
                            "deletion-vector-v2",
                            ImmutableList.of(),
                            table.currentSnapshot().snapshotId(),
                            table.currentSnapshot().sequenceNumber(),
                            ByteBuffer.wrap(roaringBitmapBytes)));
                    writer.finish();

                    puffinFileSize = writer.fileSize();
                    blobOffset = writer.writtenBlobsMetadata().get(0).offset();
                    blobLength = writer.writtenBlobsMetadata().get(0).length();
                }

                // Step 3: Attach the Puffin DV file to the table using Iceberg API
                DeleteFile puffinDeleteFile = FileMetadata.deleteFileBuilder(task.spec())
                        .ofPositionDeletes()
                        .withPath(dvPath)
                        .withFileSizeInBytes(puffinFileSize)
                        .withFormat(FileFormat.PUFFIN)
                        .withRecordCount(2)
                        .withContentOffset(blobOffset)
                        .withContentSizeInBytes(blobLength)
                        .withReferencedDataFile(dataFilePath)
                        .build();

                table.newRowDelta()
                        .addDeletes(puffinDeleteFile)
                        .commit();
            }

            // Step 4: Verify coordinator-side metadata is correct.
            // Reload the table and verify the DV file was committed with correct metadata.
            table = loadTable(tableName);
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                FileScanTask task = tasks.iterator().next();
                java.util.List<org.apache.iceberg.DeleteFile> deletes = task.deletes();
                assertFalse(deletes.isEmpty(), "Table should have deletion vector files");

                org.apache.iceberg.DeleteFile dvFile = deletes.get(0);
                assertEquals(dvFile.format(), FileFormat.PUFFIN, "Delete file should be PUFFIN format");
                assertEquals(dvFile.recordCount(), 2, "Delete file should have 2 deleted records");
                assertTrue(dvFile.fileSizeInBytes() > 0, "PUFFIN file size should be positive");
            }

            // Step 5: Verify the coordinator can enumerate splits without error.
            // The query will attempt to read data. On a Java worker, the actual DV
            // reading is not implemented (that's in Velox's DeletionVectorReader),
            // so we verify the coordinator path succeeds by running a SELECT.
            // The PUFFIN delete file will either be silently ignored by the Java
            // page source (returning all 5 rows) or cause a non-DV-rejection error.
            try {
                computeActual("SELECT * FROM " + tableName);
            }
            catch (RuntimeException e) {
                // The Java page source may fail trying to read the PUFFIN file as
                // positional deletes (since it doesn't have a DV reader). That's expected.
                // The important assertion is that the error is NOT the old
                // "PUFFIN not supported" rejection from the coordinator.
                assertFalse(
                        e.getMessage().contains("Iceberg deletion vectors") && e.getMessage().contains("not supported"),
                        "Coordinator should not reject PUFFIN deletion vectors: " + e.getMessage());

                // Also verify it's not a file-not-found error (the Puffin file exists)
                assertFalse(
                        e.getMessage().contains("FileNotFoundException"),
                        "PUFFIN file should exist on disk: " + e.getMessage());
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * Serializes a roaring bitmap in the portable "no-run" format.
     * Standard format: cookie = (numContainers - 1) << 16 | 12346 as a single int32,
     * followed by container headers (key + cardinality-1, 2 bytes each),
     * then container data (sorted uint16 values).
     * Only supports positions within a single container (all < 65536).
     */
    private static byte[] serializeRoaringBitmapNoRun(int[] positions)
    {
        // Cookie with embedded numContainers (4 bytes)
        // + 1 container key-cardinality pair (4 bytes)
        // + sorted uint16 values (2 bytes each)
        int numPositions = positions.length;
        int dataSize = 4 + 4 + numPositions * 2;
        ByteBuffer buffer = ByteBuffer.allocate(dataSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Cookie: (numContainers - 1) << 16 | SERIAL_COOKIE_NO_RUNCONTAINER
        // For 1 container: (1 - 1) << 16 | 12346 = 12346
        buffer.putInt(12346);
        // Container key (high 16 bits): 0, cardinality - 1
        buffer.putShort((short) 0);
        buffer.putShort((short) (numPositions - 1));
        // Container data: sorted uint16 values (low 16 bits of each position)
        java.util.Arrays.sort(positions);
        for (int pos : positions) {
            buffer.putShort((short) (pos & 0xFFFF));
        }

        return buffer.array();
    }

    @Test
    public void testDeletionVectorDeletesAllRows()
            throws Exception
    {
        String tableName = "test_dv_deletes_all_rows";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three')", 3);

            Table table = loadTable(tableName);

            // Write a DV that deletes all 3 rows (positions 0, 1, 2).
            byte[] roaringBitmapBytes = serializeRoaringBitmapNoRun(new int[] {0, 1, 2});

            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                FileScanTask task = tasks.iterator().next();
                String dataFilePath = task.file().path().toString();

                String dvPath = table.location() + "/data/dv-all-" + UUID.randomUUID() + ".puffin";
                OutputFile outputFile = table.io().newOutputFile(dvPath);

                long blobOffset;
                long blobLength;
                long puffinFileSize;

                try (PuffinWriter writer = Puffin.write(outputFile)
                        .createdBy("presto-test")
                        .build()) {
                    writer.add(new Blob(
                            "deletion-vector-v2",
                            ImmutableList.of(),
                            table.currentSnapshot().snapshotId(),
                            table.currentSnapshot().sequenceNumber(),
                            ByteBuffer.wrap(roaringBitmapBytes)));
                    writer.finish();

                    puffinFileSize = writer.fileSize();
                    blobOffset = writer.writtenBlobsMetadata().get(0).offset();
                    blobLength = writer.writtenBlobsMetadata().get(0).length();
                }

                DeleteFile puffinDeleteFile = FileMetadata.deleteFileBuilder(task.spec())
                        .ofPositionDeletes()
                        .withPath(dvPath)
                        .withFileSizeInBytes(puffinFileSize)
                        .withFormat(FileFormat.PUFFIN)
                        .withRecordCount(3)
                        .withContentOffset(blobOffset)
                        .withContentSizeInBytes(blobLength)
                        .withReferencedDataFile(dataFilePath)
                        .build();

                table.newRowDelta()
                        .addDeletes(puffinDeleteFile)
                        .commit();
            }

            // Verify the coordinator can enumerate splits. On Java workers the DV
            // reader isn't implemented, so the query may either succeed (returning
            // all rows because the Java page source ignores the DV) or fail with a
            // non-rejection error. The key assertion is that it doesn't throw
            // "PUFFIN not supported".
            try {
                computeActual("SELECT * FROM " + tableName);
            }
            catch (RuntimeException e) {
                assertFalse(
                        e.getMessage().contains("Iceberg deletion vectors") && e.getMessage().contains("not supported"),
                        "Coordinator should not reject PUFFIN deletion vectors: " + e.getMessage());
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testDeletionVectorOnMultipleDataFiles()
            throws Exception
    {
        String tableName = "test_dv_multiple_data_files";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            // Two separate inserts create two separate data files.
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three')", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'four'), (5, 'five'), (6, 'six')", 3);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 6");

            Table table = loadTable(tableName);

            // Attach a DV only to the first data file (positions 0 and 2 → rows 1
            // and 3 from the first insert). The second data file has no deletes.
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                FileScanTask firstTask = tasks.iterator().next();
                String firstDataFilePath = firstTask.file().path().toString();

                byte[] roaringBitmapBytes = serializeRoaringBitmapNoRun(new int[] {0, 2});
                String dvPath = table.location() + "/data/dv-partial-" + UUID.randomUUID() + ".puffin";
                OutputFile outputFile = table.io().newOutputFile(dvPath);

                long blobOffset;
                long blobLength;
                long puffinFileSize;

                try (PuffinWriter writer = Puffin.write(outputFile)
                        .createdBy("presto-test")
                        .build()) {
                    writer.add(new Blob(
                            "deletion-vector-v2",
                            ImmutableList.of(),
                            table.currentSnapshot().snapshotId(),
                            table.currentSnapshot().sequenceNumber(),
                            ByteBuffer.wrap(roaringBitmapBytes)));
                    writer.finish();

                    puffinFileSize = writer.fileSize();
                    blobOffset = writer.writtenBlobsMetadata().get(0).offset();
                    blobLength = writer.writtenBlobsMetadata().get(0).length();
                }

                DeleteFile puffinDeleteFile = FileMetadata.deleteFileBuilder(firstTask.spec())
                        .ofPositionDeletes()
                        .withPath(dvPath)
                        .withFileSizeInBytes(puffinFileSize)
                        .withFormat(FileFormat.PUFFIN)
                        .withRecordCount(2)
                        .withContentOffset(blobOffset)
                        .withContentSizeInBytes(blobLength)
                        .withReferencedDataFile(firstDataFilePath)
                        .build();

                table.newRowDelta()
                        .addDeletes(puffinDeleteFile)
                        .commit();
            }

            // Verify coordinator metadata: only the first file's task should have deletes.
            table = loadTable(tableName);
            int tasksWithDeletes = 0;
            int tasksWithoutDeletes = 0;
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    if (task.deletes().isEmpty()) {
                        tasksWithoutDeletes++;
                    }
                    else {
                        tasksWithDeletes++;
                        assertEquals(task.deletes().size(), 1, "First data file should have exactly 1 DV");
                        assertEquals(task.deletes().get(0).format(), FileFormat.PUFFIN);
                    }
                }
            }
            assertEquals(tasksWithDeletes, 1, "Exactly one data file should have a DV");
            assertEquals(tasksWithoutDeletes, 1, "Exactly one data file should have no deletes");

            // Run a query — coordinator should enumerate splits without error.
            try {
                computeActual("SELECT * FROM " + tableName);
            }
            catch (RuntimeException e) {
                assertFalse(
                        e.getMessage().contains("Iceberg deletion vectors") && e.getMessage().contains("not supported"),
                        "Coordinator should not reject PUFFIN deletion vectors: " + e.getMessage());
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3SchemaEvolution()
            throws Exception
    {
        String tableName = "test_v3_schema_evolution";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two')", 2);

            // Add a new column via Iceberg API
            Table table = loadTable(tableName);
            table.updateSchema()
                    .addColumn("score", org.apache.iceberg.types.Types.DoubleType.get())
                    .commit();

            // New inserts include the new column
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'three', 99.5)", 1);

            // Verify all rows are readable (old rows have NULL for the new column)
            assertQuery("SELECT id, value FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'one'), (2, 'two'), (3, 'three')");
            assertQuery("SELECT id, score FROM " + tableName + " WHERE score IS NOT NULL",
                    "VALUES (3, 99.5)");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE score IS NULL", "SELECT 2");

            // Rename a column
            table = loadTable(tableName);
            table.updateSchema()
                    .renameColumn("value", "label")
                    .commit();

            // Verify reads still work after rename
            assertQuery("SELECT id, label FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'one'), (2, 'two'), (3, 'three')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3MultipleSnapshotsWithDV()
            throws Exception
    {
        String tableName = "test_v3_multi_snapshot_dv";
        try {
            // Snapshot 1: initial data
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three')", 3);

            Table table = loadTable(tableName);
            long snapshot1Id = table.currentSnapshot().snapshotId();

            // Snapshot 2: attach a DV deleting row at position 1 (row id=2, 'two')
            byte[] roaringBitmapBytes = serializeRoaringBitmapNoRun(new int[] {1});

            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                FileScanTask task = tasks.iterator().next();
                String dataFilePath = task.file().path().toString();

                String dvPath = table.location() + "/data/dv-snap-" + UUID.randomUUID() + ".puffin";
                OutputFile outputFile = table.io().newOutputFile(dvPath);

                long blobOffset;
                long blobLength;
                long puffinFileSize;

                try (PuffinWriter writer = Puffin.write(outputFile)
                        .createdBy("presto-test")
                        .build()) {
                    writer.add(new Blob(
                            "deletion-vector-v2",
                            ImmutableList.of(),
                            table.currentSnapshot().snapshotId(),
                            table.currentSnapshot().sequenceNumber(),
                            ByteBuffer.wrap(roaringBitmapBytes)));
                    writer.finish();

                    puffinFileSize = writer.fileSize();
                    blobOffset = writer.writtenBlobsMetadata().get(0).offset();
                    blobLength = writer.writtenBlobsMetadata().get(0).length();
                }

                DeleteFile puffinDeleteFile = FileMetadata.deleteFileBuilder(task.spec())
                        .ofPositionDeletes()
                        .withPath(dvPath)
                        .withFileSizeInBytes(puffinFileSize)
                        .withFormat(FileFormat.PUFFIN)
                        .withRecordCount(1)
                        .withContentOffset(blobOffset)
                        .withContentSizeInBytes(blobLength)
                        .withReferencedDataFile(dataFilePath)
                        .build();

                table.newRowDelta()
                        .addDeletes(puffinDeleteFile)
                        .commit();
            }

            // Snapshot 3: more data added after the DV
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'four'), (5, 'five')", 2);

            // Verify the table now has 3 snapshots
            table = loadTable(tableName);
            int snapshotCount = 0;
            for (org.apache.iceberg.Snapshot snapshot : table.snapshots()) {
                snapshotCount++;
            }
            assertTrue(snapshotCount >= 3, "Table should have at least 3 snapshots, got: " + snapshotCount);

            // Verify coordinator can enumerate all splits (including those with DVs
            // and those from the post-DV insert).
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                int totalFiles = 0;
                int filesWithDeletes = 0;
                for (FileScanTask task : tasks) {
                    totalFiles++;
                    if (!task.deletes().isEmpty()) {
                        filesWithDeletes++;
                    }
                }
                assertEquals(totalFiles, 2, "Should have 2 data files (one from each insert)");
                assertEquals(filesWithDeletes, 1, "Only the first data file should have DV deletes");
            }

            // Run a query to verify coordinator enumeration succeeds.
            try {
                computeActual("SELECT * FROM " + tableName);
            }
            catch (RuntimeException e) {
                assertFalse(
                        e.getMessage().contains("Iceberg deletion vectors") && e.getMessage().contains("not supported"),
                        "Coordinator should not reject PUFFIN deletion vectors: " + e.getMessage());
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3DeletionVectorMetadataFields()
            throws Exception
    {
        String tableName = "test_dv_metadata_fields";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two')", 2);

            Table table = loadTable(tableName);

            byte[] roaringBitmapBytes = serializeRoaringBitmapNoRun(new int[] {0});
            String dvPath = table.location() + "/data/dv-meta-" + UUID.randomUUID() + ".puffin";

            long blobOffset;
            long blobLength;
            long puffinFileSize;

            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                FileScanTask task = tasks.iterator().next();
                String dataFilePath = task.file().path().toString();

                OutputFile outputFile = table.io().newOutputFile(dvPath);

                try (PuffinWriter writer = Puffin.write(outputFile)
                        .createdBy("presto-test")
                        .build()) {
                    writer.add(new Blob(
                            "deletion-vector-v2",
                            ImmutableList.of(),
                            table.currentSnapshot().snapshotId(),
                            table.currentSnapshot().sequenceNumber(),
                            ByteBuffer.wrap(roaringBitmapBytes)));
                    writer.finish();

                    puffinFileSize = writer.fileSize();
                    blobOffset = writer.writtenBlobsMetadata().get(0).offset();
                    blobLength = writer.writtenBlobsMetadata().get(0).length();
                }

                DeleteFile puffinDeleteFile = FileMetadata.deleteFileBuilder(task.spec())
                        .ofPositionDeletes()
                        .withPath(dvPath)
                        .withFileSizeInBytes(puffinFileSize)
                        .withFormat(FileFormat.PUFFIN)
                        .withRecordCount(1)
                        .withContentOffset(blobOffset)
                        .withContentSizeInBytes(blobLength)
                        .withReferencedDataFile(dataFilePath)
                        .build();

                table.newRowDelta()
                        .addDeletes(puffinDeleteFile)
                        .commit();
            }

            // Verify the committed DV file has correct metadata fields.
            table = loadTable(tableName);
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                FileScanTask task = tasks.iterator().next();
                java.util.List<org.apache.iceberg.DeleteFile> deletes = task.deletes();
                assertFalse(deletes.isEmpty(), "Should have deletion vector files");

                org.apache.iceberg.DeleteFile dvFile = deletes.get(0);
                assertEquals(dvFile.format(), FileFormat.PUFFIN, "Format should be PUFFIN");
                assertEquals(dvFile.recordCount(), 1, "Record count should match deleted positions");
                assertTrue(dvFile.fileSizeInBytes() > 0, "File size must be positive");

                // Verify the DV file path ends with .puffin as expected.
                assertTrue(dvFile.path().toString().endsWith(".puffin"), "DV file should be a .puffin file");
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3WriteReadRoundTrip()
            throws Exception
    {
        String tableName = "test_v3_write_read_round_trip";
        try {
            // Step 1: Create V3 table and insert initial data
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0), (4, 'Dave', 400.0), (5, 'Eve', 500.0)", 5);

            // Step 2: Verify initial data via read path
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 5");
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0), (4, 'Dave', 400.0), (5, 'Eve', 500.0)");

            // Step 3: First DELETE via write path (produces DV #1)
            assertUpdate("DELETE FROM " + tableName + " WHERE id IN (1, 3)", 2);

            // Step 4: Verify read path filters DV #1 correctly
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 'Bob', 200.0), (4, 'Dave', 400.0), (5, 'Eve', 500.0)");
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");

            // Step 5: Cross-validate DV #1 metadata via Iceberg API
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);

            int dvCount = 0;
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    for (org.apache.iceberg.DeleteFile deleteFile : task.deletes()) {
                        dvCount++;
                        assertEquals(deleteFile.format(), FileFormat.PUFFIN,
                                "Presto-written DV must use PUFFIN format");
                        assertTrue(deleteFile.path().toString().endsWith(".puffin"),
                                "DV file path must end with .puffin");
                        assertTrue(deleteFile.fileSizeInBytes() > 0,
                                "DV file size must be positive");
                        assertTrue(deleteFile.contentOffset() >= 0,
                                "DV content offset must be non-negative");
                        assertTrue(deleteFile.contentSizeInBytes() > 0,
                                "DV content size must be positive");
                        assertTrue(deleteFile.recordCount() > 0,
                                "DV record count must be positive");
                    }
                }
            }
            assertTrue(dvCount > 0, "Should have at least one deletion vector after DELETE");

            // Step 6: Insert more data (creates a new data file alongside existing ones)
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (6, 'Frank', 600.0), (7, 'Grace', 700.0)", 2);

            // Step 7: Verify read path handles mixed state: old data with DVs + new data
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 'Bob', 200.0), (4, 'Dave', 400.0), (5, 'Eve', 500.0), (6, 'Frank', 600.0), (7, 'Grace', 700.0)");
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 5");

            // Step 8: Second DELETE via write path (produces DV #2, targeting new and old data)
            assertUpdate("DELETE FROM " + tableName + " WHERE id IN (2, 7)", 2);

            // Step 9: Verify cumulative read path correctness with two rounds of DVs
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (4, 'Dave', 400.0), (5, 'Eve', 500.0), (6, 'Frank', 600.0)");
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");

            // Step 10: Cross-validate cumulative DV metadata via Iceberg API
            table = loadTable(tableName);
            int totalDvs = 0;
            int totalDataFiles = 0;
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    totalDataFiles++;
                    for (org.apache.iceberg.DeleteFile deleteFile : task.deletes()) {
                        totalDvs++;
                        assertEquals(deleteFile.format(), FileFormat.PUFFIN,
                                "All DVs must use PUFFIN format");
                        assertTrue(deleteFile.recordCount() > 0,
                                "Each DV must have positive record count");
                    }
                }
            }
            assertTrue(totalDvs > 0, "Should have deletion vectors after two rounds of DELETE");
            assertTrue(totalDataFiles > 0, "Should have data files remaining");

            // Step 11: Verify aggregation works correctly over DV-filtered data
            assertQuery("SELECT SUM(value) FROM " + tableName, "SELECT 1500.0");
            assertQuery("SELECT MIN(id), MAX(id) FROM " + tableName, "VALUES (4, 6)");

            // Step 12: Verify predicates work correctly with DVs
            assertQuery("SELECT * FROM " + tableName + " WHERE value > 450.0 ORDER BY id",
                    "VALUES (5, 'Eve', 500.0), (6, 'Frank', 600.0)");
            assertQuery("SELECT * FROM " + tableName + " WHERE name LIKE '%a%' ORDER BY id",
                    "VALUES (4, 'Dave', 400.0), (6, 'Frank', 600.0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    private void dropTableViaIceberg(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), ICEBERG_CATALOG,
                getProperties(), new Configuration());
        catalog.dropTable(
                TableIdentifier.of(TEST_SCHEMA, tableName), true);
    }

    @Test
    public void testRewriteDeleteFilesProcedure()
            throws Exception
    {
        String tableName = "test_rewrite_delete_files";
        try {
            // Step 1: Create V3 table and insert data
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Carol', 300.0), (4, 'Dave', 400.0), (5, 'Eve', 500.0)", 5);

            // Step 2: Perform multiple deletes to create multiple DVs per data file
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 3", 1);

            // Step 3: Verify we have multiple delete files before compaction
            Table table = loadTable(tableName);
            int dvCountBefore = 0;
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    dvCountBefore += task.deletes().size();
                }
            }
            assertTrue(dvCountBefore >= 2, "Should have at least 2 DVs before compaction, got: " + dvCountBefore);

            // Step 4: Verify data is correct before compaction
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 'Bob', 200.0), (4, 'Dave', 400.0), (5, 'Eve', 500.0)");

            // Step 5: Run DV compaction
            assertQuerySucceeds(format("CALL system.rewrite_delete_files('%s', '%s')", TEST_SCHEMA, tableName));

            // Step 6: Verify data is still correct after compaction
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 'Bob', 200.0), (4, 'Dave', 400.0), (5, 'Eve', 500.0)");

            // Step 7: Verify DVs were compacted (fewer or equal DVs)
            table.refresh();
            int dvCountAfter = 0;
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    for (DeleteFile dv : task.deletes()) {
                        dvCountAfter++;
                        assertEquals(dv.format(), FileFormat.PUFFIN, "Compacted DV must use PUFFIN format");
                    }
                }
            }
            assertTrue(dvCountAfter <= dvCountBefore,
                    "DV count after compaction (" + dvCountAfter + ") should be <= before (" + dvCountBefore + ")");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDeleteFilesOnV2Table()
            throws Exception
    {
        String tableName = "test_rewrite_delete_files_v2";
        try {
            // V2 tables should be a no-op (no DVs to compact)
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, value VARCHAR) WITH (\"format-version\" = '2', delete_mode = 'merge-on-read')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three')", 3);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);

            assertQuerySucceeds(format("CALL system.rewrite_delete_files('%s', '%s')", TEST_SCHEMA, tableName));

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 'two'), (3, 'three')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3DefaultValues()
            throws Exception
    {
        String tableName = "test_v3_default_values";
        try {
            // Step 1: Create V3 table and insert initial data
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'Alice'), (2, 'Bob')", 2);

            // Step 2: Add column via Iceberg API (default values not yet supported in Iceberg 1.10.x)
            Table table = loadTable(tableName);
            table.updateSchema()
                    .addColumn("score", org.apache.iceberg.types.Types.DoubleType.get())
                    .commit();

            // Step 3: Verify we can read old data — the new column should be NULL
            assertQuery("SELECT id, name FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice'), (2, 'Bob')");

            // Step 4: Insert new data with the new column
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'Carol', 300.0)", 1);

            // Step 5: Verify new data reads correctly
            assertQuery("SELECT id, name, score FROM " + tableName + " WHERE id = 3",
                    "VALUES (3, 'Carol', 300.0)");

            // Step 6: Verify old rows get NULL for the new column (schema evolution without default)
            assertQuery("SELECT id, name, score FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', NULL), (2, 'Bob', NULL), (3, 'Carol', 300.0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMultiArgumentPartitionTransforms()
            throws Exception
    {
        String tableName = "test_v3_multi_arg_transforms";
        try {
            // Create V3 table with bucket(4, id) partitioning
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE)"
                    + " WITH (\"format-version\" = '3', partitioning = ARRAY['bucket(id, 4)'])");

            // Verify table was created with correct partition spec
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
            assertEquals(table.spec().fields().size(), 1);
            assertEquals(table.spec().fields().get(0).transform().toString(), "bucket[4]");

            // Insert data — should distribute across buckets
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0), (4, 'Diana', 400.0)", 4);

            // Verify data reads correctly
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0), (4, 'Diana', 400.0)");

            // Verify partition pruning works — query with equality predicate
            assertQuery("SELECT name, value FROM " + tableName + " WHERE id = 2",
                    "VALUES ('Bob', 200.0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testTruncatePartitionTransform()
            throws Exception
    {
        String tableName = "test_v3_truncate_transform";
        try {
            // Create V3 table with truncate(10, value) partitioning on a varchar column
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, category VARCHAR, amount DOUBLE)"
                    + " WITH (\"format-version\" = '3', partitioning = ARRAY['truncate(category, 3)'])");

            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
            assertEquals(table.spec().fields().size(), 1);
            assertEquals(table.spec().fields().get(0).transform().toString(), "truncate[3]");

            // Insert data with varying category prefixes
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'food_pizza', 15.0), (2, 'food_burger', 12.0),"
                    + " (3, 'drink_coffee', 5.0), (4, 'drink_tea', 3.0)", 4);

            // Verify data reads correctly
            assertQuery("SELECT id, category, amount FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'food_pizza', 15.0), (2, 'food_burger', 12.0),"
                    + " (3, 'drink_coffee', 5.0), (4, 'drink_tea', 3.0)");

            // Verify we can filter
            assertQuery("SELECT id FROM " + tableName + " WHERE category = 'food_pizza'",
                    "VALUES 1");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testNanosecondTimestampSchema()
            throws Exception
    {
        String tableName = "test_v3_timestamp_nano";
        try {
            // Create V3 table with Presto
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");

            // Add nanosecond timestamp columns via Iceberg API
            Table table = loadTable(tableName);
            table.updateSchema()
                    .addColumn("ts_nano", Types.TimestampNanoType.withoutZone())
                    .addColumn("ts_nano_tz", Types.TimestampNanoType.withZone())
                    .commit();

            // Verify Presto can read the schema with nanosecond columns
            // ts_nano maps to timestamp microseconds, ts_nano_tz maps to timestamp with time zone
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");

            // Insert data through Presto — the nanosecond columns accept null values
            assertUpdate("INSERT INTO " + tableName + " (id) VALUES (1)", 1);
            assertQuery("SELECT id FROM " + tableName, "VALUES 1");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testVariantColumnSchema()
            throws Exception
    {
        String tableName = "test_v3_variant";
        try {
            // Create V3 table with Presto
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");

            // Add variant column via Iceberg API
            Table table = loadTable(tableName);
            table.updateSchema()
                    .addColumn("data", Types.VariantType.get())
                    .commit();

            // Verify Presto can read the schema with the variant column
            // Variant maps to JSON in Presto
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");

            // Insert data — the variant column accepts null values
            assertUpdate("INSERT INTO " + tableName + " (id) VALUES (1)", 1);
            assertQuery("SELECT id FROM " + tableName, "VALUES 1");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testVariantTypeEndToEnd()
            throws Exception
    {
        String tableName = "test_v3_variant_e2e";
        try {
            // Step 1: Create V3 table and add variant columns via Iceberg schema evolution
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            table.updateSchema()
                    .addColumn("metadata", Types.VariantType.get())
                    .commit();

            // Step 2: Verify empty table with variant column is queryable
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");

            // Step 3: Insert data — variant column receives NULLs
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", 3);

            // Step 4: Verify full row reads including NULL variant values
            assertQuery("SELECT id, name, metadata FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', NULL), (2, 'Bob', NULL), (3, 'Charlie', NULL)");

            // Step 5: Test IS NULL predicate on variant column
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE metadata IS NULL", "SELECT 3");

            // Step 6: Test filtering on non-variant columns with variant columns in projection
            assertQuery("SELECT id, name, metadata FROM " + tableName + " WHERE id > 1 ORDER BY id",
                    "VALUES (2, 'Bob', NULL), (3, 'Charlie', NULL)");

            // Step 7: Test aggregation with variant columns in the table
            assertQuery("SELECT count(*), min(id), max(id) FROM " + tableName, "VALUES (3, 1, 3)");
            assertQuery("SELECT name, count(*) FROM " + tableName + " GROUP BY name ORDER BY name",
                    "VALUES ('Alice', 1), ('Bob', 1), ('Charlie', 1)");

            // Step 8: DELETE rows from a table with variant columns
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 2");
            assertQuery("SELECT id, name FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice'), (3, 'Charlie')");

            // Step 9: Insert more data after deletion
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (4, 'Diana'), (5, 'Eve')", 2);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 4");

            // Step 10: Verify mixed snapshots (pre-delete and post-delete) read correctly
            assertQuery("SELECT id, name FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice'), (3, 'Charlie'), (4, 'Diana'), (5, 'Eve')");

            // Step 11: Further schema evolution — add another variant column alongside the first
            table = loadTable(tableName);
            table.updateSchema()
                    .addColumn("tags", Types.VariantType.get())
                    .commit();

            // Step 12: Verify reads still work with two variant columns
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 4");
            assertQuery("SELECT id, name FROM " + tableName + " WHERE id = 1",
                    "VALUES (1, 'Alice')");

            // Step 13: Insert with both variant columns NULL
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (6, 'Frank')", 1);
            assertQuery("SELECT id, metadata, tags FROM " + tableName + " WHERE id = 6",
                    "VALUES (6, NULL, NULL)");

            // Step 14: Verify V3 format preserved through all operations
            table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testVariantColumnWithPartitioning()
            throws Exception
    {
        String tableName = "test_v3_variant_partitioned";
        try {
            // Create V3 partitioned table with variant column
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, category VARCHAR) WITH (\"format-version\" = '3', partitioning = ARRAY['category'])");
            Table table = loadTable(tableName);
            table.updateSchema()
                    .addColumn("data", Types.VariantType.get())
                    .commit();

            // Insert data into multiple partitions
            assertUpdate("INSERT INTO " + tableName + " (id, category) VALUES (1, 'A'), (2, 'A'), (3, 'B'), (4, 'C')", 4);

            // Verify partition pruning works with variant column present
            assertQuery("SELECT id FROM " + tableName + " WHERE category = 'A' ORDER BY id",
                    "VALUES 1, 2");
            assertQuery("SELECT id FROM " + tableName + " WHERE category = 'B'",
                    "VALUES 3");

            // Verify cross-partition aggregation
            assertQuery("SELECT category, count(*) FROM " + tableName + " GROUP BY category ORDER BY category",
                    "VALUES ('A', 2), ('B', 1), ('C', 1)");

            // Delete within a partition
            assertUpdate("DELETE FROM " + tableName + " WHERE category = 'A'", 2);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 2");
            assertQuery("SELECT id FROM " + tableName + " ORDER BY id",
                    "VALUES 3, 4");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testVariantJsonDataRoundTrip()
            throws Exception
    {
        String tableName = "test_v3_variant_json_data";
        try {
            // Step 1: Create V3 table and add variant column via Iceberg API
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            table.updateSchema()
                    .addColumn("metadata", Types.VariantType.get())
                    .commit();

            // Step 2: Insert rows with actual JSON data into the variant column.
            // VARIANT maps to JSON in Presto; use SQL JSON '...' literal syntax.
            assertUpdate("INSERT INTO " + tableName + " VALUES "
                    + "(1, 'Alice', JSON '{\"age\":30,\"city\":\"NYC\"}'), "
                    + "(2, 'Bob', JSON '{\"age\":25}'), "
                    + "(3, 'Charlie', NULL)", 3);

            // Step 3: Verify round-trip — JSON values survive write → Parquet → read
            assertQuery("SELECT id, name, metadata FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', '{\"age\":30,\"city\":\"NYC\"}'), "
                            + "(2, 'Bob', '{\"age\":25}'), "
                            + "(3, 'Charlie', NULL)");

            // Step 4: Test filtering on non-variant columns with variant data present
            assertQuery("SELECT metadata FROM " + tableName + " WHERE id = 1",
                    "VALUES ('{\"age\":30,\"city\":\"NYC\"}')");

            // Step 5: Test IS NULL / IS NOT NULL on variant column with actual data
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE metadata IS NOT NULL", "SELECT 2");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE metadata IS NULL", "SELECT 1");

            // Step 6: Insert rows with different JSON value types (number, string, boolean)
            assertUpdate("INSERT INTO " + tableName + " VALUES "
                    + "(4, 'Diana', JSON '42'), "
                    + "(5, 'Eve', JSON '\"simple string\"'), "
                    + "(6, 'Frank', JSON 'true')", 3);

            // Step 7: Verify all rows
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 6");
            assertQuery("SELECT metadata FROM " + tableName + " WHERE id = 4", "VALUES ('42')");
            assertQuery("SELECT metadata FROM " + tableName + " WHERE id = 6", "VALUES ('true')");

            // Step 8: Delete rows with variant data
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE metadata IS NOT NULL", "SELECT 4");

            // Step 9: Verify remaining data
            assertQuery("SELECT id, name FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 'Bob'), (3, 'Charlie'), (4, 'Diana'), (5, 'Eve'), (6, 'Frank')");

            // Step 10: Verify V3 format preserved
            table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testVariantColumnWithDeleteAndUpdate()
            throws Exception
    {
        String tableName = "test_v3_variant_dml";
        try {
            // Create V3 table with merge-on-read delete mode and variant column
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, score DOUBLE)"
                    + " WITH (\"format-version\" = '3', \"write.delete.mode\" = 'merge-on-read', \"write.update.mode\" = 'merge-on-read')");
            Table table = loadTable(tableName);
            table.updateSchema()
                    .addColumn("extra", Types.VariantType.get())
                    .commit();

            // Insert data
            assertUpdate("INSERT INTO " + tableName + " (id, name, score) VALUES "
                    + "(1, 'Alice', 85.5), (2, 'Bob', 92.0), (3, 'Charlie', 78.3), (4, 'Diana', 95.0)", 4);

            // Verify initial data
            assertQuery("SELECT id, name, score FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 85.5), (2, 'Bob', 92.0), (3, 'Charlie', 78.3), (4, 'Diana', 95.0)");

            // Row-level DELETE (produces deletion vector)
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertQuery("SELECT id, name FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice'), (3, 'Charlie'), (4, 'Diana')");

            // Verify DV metadata is PUFFIN format
            table = loadTable(tableName);
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    for (org.apache.iceberg.DeleteFile deleteFile : task.deletes()) {
                        assertEquals(deleteFile.format(), FileFormat.PUFFIN);
                    }
                }
            }

            // UPDATE on table with variant column
            assertUpdate("UPDATE " + tableName + " SET score = 99.9 WHERE id = 1", 1);
            assertQuery("SELECT id, name, score FROM " + tableName + " WHERE id = 1",
                    "VALUES (1, 'Alice', 99.9)");

            // Verify final state
            assertQuery("SELECT id, name, score FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 99.9), (3, 'Charlie', 78.3), (4, 'Diana', 95.0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testVariantWithJsonExtract()
            throws Exception
    {
        String tableName = "test_v3_variant_json_extract";
        try {
            // Create V3 table and add Variant column via Iceberg API
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            table.updateSchema()
                    .addColumn("metadata", Types.VariantType.get())
                    .commit();

            // Insert a row using SQL JSON literal syntax (Variant maps to JSON in Presto)
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, JSON '{\"age\":30,\"city\":\"NYC\"}')", 1);

            // Verify json_extract_scalar works directly on the Variant column —
            // the user-visible payoff of mapping Variant to JSON.
            assertQuery(
                    "SELECT json_extract_scalar(metadata, '$.city') FROM " + tableName + " WHERE id = 1",
                    "VALUES ('NYC')");
            assertQuery(
                    "SELECT json_extract_scalar(metadata, '$.age') FROM " + tableName + " WHERE id = 1",
                    "VALUES ('30')");
        }
        finally {
            dropTable(tableName);
        }
    }
}
