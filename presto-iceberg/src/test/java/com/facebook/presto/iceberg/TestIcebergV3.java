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
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.OptionalInt;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

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
    public void testDeleteOnV3TableNotSupported()
    {
        String tableName = "test_v3_delete";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE) WITH (\"format-version\" = '3', \"write.delete.mode\" = 'merge-on-read')");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)", 3);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");
            assertThatThrownBy(() -> getQueryRunner().execute("DELETE FROM " + tableName + " WHERE id = 1"))
                    .hasMessageContaining("Iceberg table updates for format version 3 are not supported yet");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testTruncateV3Table()
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
    public void testUpdateOnV3TableNotSupported()
    {
        String tableName = "test_v3_update";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, status VARCHAR, score DOUBLE) WITH (\"format-version\" = '3', \"write.update.mode\" = 'merge-on-read')");
            assertUpdate("INSERT INTO " + tableName
                            + " VALUES (1, 'Alice', 'active', 85.5), (2, 'Bob', 'active', 92.0), (3, 'Charlie', 'inactive', 78.3)",
                    3);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 'active', 85.5), (2, 'Bob', 'active', 92.0), (3, 'Charlie', 'inactive', 78.3)");
            assertThatThrownBy(() -> getQueryRunner()
                    .execute("UPDATE " + tableName + " SET status = 'updated', score = 95.0 WHERE id = 1"))
                    .hasMessageContaining("Iceberg table updates for format version 3 are not supported yet");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMergeOnV3TableNotSupported()
    {
        String tableName = "test_v3_merge_target";
        String sourceTable = "test_v3_merge_source";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE) WITH (\"format-version\" = '3', \"write.update.mode\" = 'merge-on-read')");
            assertUpdate("CREATE TABLE " + sourceTable + " (id INTEGER, name VARCHAR, value DOUBLE)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", 2);
            assertUpdate("INSERT INTO " + sourceTable + " VALUES (1, 'Alice Updated', 150.0), (3, 'Charlie', 300.0)",
                    2);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)");
            assertQuery("SELECT * FROM " + sourceTable + " ORDER BY id",
                    "VALUES (1, 'Alice Updated', 150.0), (3, 'Charlie', 300.0)");
            assertThatThrownBy(() -> getQueryRunner().execute(
                    "MERGE INTO " + tableName + " t USING " + sourceTable + " s ON t.id = s.id " +
                            "WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value " +
                            "WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)"))
                    .hasMessageContaining("Iceberg table updates for format version 3 are not supported yet");
        }
        finally {
            dropTable(tableName);
            dropTable(sourceTable);
        }
    }

    @Test
    public void testOptimizeOnV3Table()
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
    public void testPuffinDeletionVectorsNotSupported()
            throws Exception
    {
        String tableName = "test_puffin_deletion_vectors_not_supported";
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

            assertQueryFails("SELECT * FROM " + tableName, "Iceberg deletion vectors.*PUFFIN.*not supported");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3SupportedOperations()
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

    private void dropTableViaIceberg(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), ICEBERG_CATALOG,
                getProperties(), new Configuration());
        catalog.dropTable(
                TableIdentifier.of(TEST_SCHEMA, tableName), true);
    }
}
