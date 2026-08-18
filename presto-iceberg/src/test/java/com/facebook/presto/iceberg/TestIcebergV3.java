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
import com.facebook.presto.common.type.RowType;
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
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.OptionalInt;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PARQUET_DEREFERENCE_PUSHDOWN_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.iceberg.IcebergUtil.MAX_FORMAT_VERSION_FOR_METADATA_TABLES;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
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

            assertQuerySucceeds(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))", TEST_SCHEMA, tableName));

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0), (4, 'C', 300.0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMetadataTablesThrowOnUnsupportedFormatVersion()
    {
        // Tests unsupported format versions throw clear errors instead of silent data loss
        int unsupportedVersion = MAX_FORMAT_VERSION_FOR_METADATA_TABLES + 1;
        String tableName = "test_unsupported_version_table";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, category VARCHAR, value DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A', 100.0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'B', 200.0)", 1);
            Table table = loadTable(tableName);
            table.updateProperties().set("format-version", String.valueOf(unsupportedVersion)).commit();
            assertQueryFails("SELECT * FROM \"" + tableName + "$files\"",
                    format("Cannot read Iceberg manifest files for table format version %s \\(max supported: %s\\).*",
                            unsupportedVersion, MAX_FORMAT_VERSION_FOR_METADATA_TABLES));
            assertQueryFails("SELECT * FROM \"" + tableName + "$partitions\"",
                    format("Cannot read Iceberg manifest files for table format version %s \\(max supported: %s\\).*",
                            unsupportedVersion, MAX_FORMAT_VERSION_FOR_METADATA_TABLES));
            assertQueryFails("SELECT * FROM \"" + tableName + "$manifests\"",
                    format("Cannot read Iceberg manifest files for table format version %s \\(max supported: %s\\).*",
                            unsupportedVersion, MAX_FORMAT_VERSION_FOR_METADATA_TABLES));
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

    @Test
    public void testAddColumnWithDefaultRequiresV3()
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

    @Test
    public void testSetColumnDefaultRequiresV3()
    {
        String tableName = "test_set_column_default_v2";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '2')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 2);
            // Try to set default on V2 table - should fail with V3 requirement error
            assertQueryFails("ALTER TABLE " + tableName + " ALTER COLUMN name SET DEFAULT 'test'",
                    "SET COLUMN DEFAULT is only supported with Iceberg format version 3 or higher.*");

            // Upgrade to V3
            BaseTable baseTable = (BaseTable) table;
            TableOperations operations = baseTable.operations();
            TableMetadata currentMetadata = operations.current();
            operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(3));
            table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);

            // Add column with initial-default in V3
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'UK'");
            table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "UK");
            assertEquals(table.schema().findField("country").writeDefault(), "UK");

            // Now update write-default only (initial-default should remain 'UK')
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN country SET DEFAULT 'US'");
            table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "UK");
            assertEquals(table.schema().findField("country").writeDefault(), "US");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSetColumnDefaultToNull()
    {
        String tableName = "test_set_column_default_null";
        try {
            // Create V3 table with a column that has a default value
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN name VARCHAR DEFAULT 'default_name'");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
            // Verify initial default is set
            assertEquals(table.schema().findField("name").initialDefault(), "default_name");
            assertEquals(table.schema().findField("name").writeDefault(), "default_name");
            // Set default to NULL - this should not throw NPE and should clear the write-default
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN name SET DEFAULT NULL");
            table = loadTable(tableName);
            // Verify initial-default remains but write-default is now null
            assertEquals(table.schema().findField("name").initialDefault(), "default_name");
            assertNull(table.schema().findField("name").writeDefault());
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertWithWriteDefault()
    {
        String tableName = "test_insert_with_write_default";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3')");
            // Add a column with default value
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'US'");
            // Verify the default is set
            Table table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "US");
            assertEquals(table.schema().findField("country").writeDefault(), "US");
            // Insert without specifying the country column - should use write-default
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (1, 'Alice')", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'Alice', 'US')");
            // Change the write-default (initial-default remains 'US')
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN country SET DEFAULT 'UK'");
            table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "US");
            assertEquals(table.schema().findField("country").writeDefault(), "UK");
            // Insert again without specifying country - should use new write-default 'UK'
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (2, 'Bob')", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, 'Alice', 'US'), (2, 'Bob', 'UK')");
            // Insert with explicit value - should override default
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'Charlie', 'CA')", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, 'Alice', 'US'), (2, 'Bob', 'UK'), (3, 'Charlie', 'CA')");
            // Set write-default to NULL
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN country SET DEFAULT NULL");
            table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "US");
            assertNull(table.schema().findField("country").writeDefault());
            // Insert without specifying country - should preserve materialized NULL, not initial-default
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (4, 'Dave')", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 'US'), (2, 'Bob', 'UK'), (3, 'Charlie', 'CA'), (4, 'Dave', NULL)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @DataProvider(name = "withPartitioning")
    public String[][] withPartitioning()
    {
        return new String[][] {
                {"PARQUET", ""},
                {"PARQUET", " WITH(partitioning = 'identity')"},
                {"ORC", ""},
                {"ORC", " WITH(partitioning = 'identity')"}
        };
    }
    @Test(dataProvider = "withPartitioning")
    public void testInsertWithPartitionEvolution(String fileFormat, String withPartitioning)
    {
        String tableName = "test_insert_with_write_default_" + fileFormat.toLowerCase() + (withPartitioning.isEmpty() ? "_unpartitioned" : "_partitioned");
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            assertUpdate("INSERT INTO " + tableName + " VALUES(1, 'Alice'), (2, 'Bob')", 2);
            // Add a column with default value
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'US'" + withPartitioning);
            // Verify the default is set
            Table table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "US");
            assertEquals(table.schema().findField("country").writeDefault(), "US");
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN country SET DEFAULT 'UK'");
            // Insert without specifying the country column - should use write-default
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (3, 'Carol')", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US'), (3, 'Carol', 'UK')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country = 'US'", "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country = 'UK'", "VALUES(3, 'Carol', 'UK')");
            assertUpdate("INSERT INTO " + tableName + " (id, name, country) VALUES (4, 'David', NULL), (5, 'Frank', 'FR')", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US'), (3, 'Carol', 'UK'), (4, 'David', NULL), (5, 'Frank', 'FR')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country = 'US'", "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country <> 'US'", "VALUES(3, 'Carol', 'UK'), (5, 'Frank', 'FR')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country = 'UK'", "VALUES(3, 'Carol', 'UK')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country IS NULL", "VALUES(4, 'David', NULL)");
            assertQuery("SELECT * FROM " + tableName + " WHERE country IS NOT NULL", "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US'), (3, 'Carol', 'UK'), (5, 'Frank', 'FR')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country in ('US', 'FR', 'CN')", "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US'), (5, 'Frank', 'FR')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertWithExplicitNullOverridesWriteDefault()
    {
        String tableName = "test_insert_explicit_null_overrides_write_default";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN status VARCHAR DEFAULT 'ACTIVE'");

            assertUpdate("INSERT INTO " + tableName + " (id, status) VALUES (1, NULL)", 1);
            assertUpdate("INSERT INTO " + tableName + " (id) VALUES (2)", 1);

            assertQuery("SELECT id, status FROM " + tableName + " ORDER BY id",
                    "VALUES (1, NULL), (2, 'ACTIVE')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertWithMultipleWriteDefaultColumns()
    {
        String tableName = "test_insert_multiple_write_default_columns";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'US'");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN priority INTEGER DEFAULT 10");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN is_enabled BOOLEAN DEFAULT true");

            assertUpdate("INSERT INTO " + tableName + " (id) VALUES (1)", 1);
            assertUpdate("INSERT INTO " + tableName + " (id, country) VALUES (2, 'UK')", 1);

            assertQuery("SELECT id, country, priority, is_enabled FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'US', 10, true), (2, 'UK', 10, true)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertWithWriteDefaultDifferentDataTypes()
    {
        String tableName = "test_insert_write_default_different_types";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN name VARCHAR DEFAULT 'Unknown'");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN score DOUBLE DEFAULT 0.0");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN count BIGINT DEFAULT 0");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN active BOOLEAN DEFAULT false");

            assertUpdate("INSERT INTO " + tableName + " (id) VALUES (1)", 1);

            assertQuery("SELECT id, name, score, count, active FROM " + tableName,
                    "VALUES (1, 'Unknown', 0.0, 0, false)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertWithWriteDefaultOnPartitionedTable()
    {
        String tableName = "test_insert_write_default_partitioned_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, ds DATE) WITH (\"format-version\" = '3', format = 'PARQUET', partitioning = ARRAY['ds'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, DATE '2023-01-01')", 1);
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN region VARCHAR DEFAULT 'US'");

            assertUpdate("INSERT INTO " + tableName + " (id, ds) VALUES (2, DATE '2023-01-02')", 1);
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN region SET DEFAULT 'EU'");
            assertUpdate("INSERT INTO " + tableName + " (id, ds) VALUES (3, DATE '2023-01-03')", 1);

            assertQuery("SELECT id, ds, region FROM " + tableName + " ORDER BY id",
                    "VALUES (1, DATE '2023-01-01', 'US'), (2, DATE '2023-01-02', 'US'), (3, DATE '2023-01-03', 'EU')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @DataProvider(name = "fileFormats")
    public Object[][] fileFormats()
    {
        return new Object[][] {{"PARQUET"}, {"ORC"}};
    }

    @Test(dataProvider = "fileFormats")
    public void testReadUnknownColumn(String fileFormat)
    {
        String tableName = "test_read_unknown_column_" + fileFormat.toLowerCase(ENGLISH);
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'Alice'), (2, 'Bob')", 2);
            addUnknownColumn(tableName, "unknown_column", Types.UnknownType.get());

            // An unknown column is never stored in a data file, so it always reads back as null
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'Alice', NULL), (2, 'Bob', NULL)");
            assertQuery("SELECT unknown_column FROM " + tableName, "VALUES NULL, NULL");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 2");
            assertQuery("SELECT count(unknown_column) FROM " + tableName, "VALUES 0");
            assertEquals(
                    getQueryRunner().execute("SELECT unknown_column FROM " + tableName).getTypes(),
                    ImmutableList.of(UNKNOWN));

            // Iceberg binds IS NULL on an unknown column to alwaysTrue and IS NOT NULL to alwaysFalse
            assertQuery("SELECT id FROM " + tableName + " WHERE unknown_column IS NULL", "VALUES 1, 2");
            assertQuery("SELECT id FROM " + tableName + " WHERE unknown_column IS NOT NULL", "SELECT 1 WHERE FALSE");

            // Queries that do not reference the unknown column must still plan
            assertQuery("SELECT id FROM " + tableName + " WHERE name = 'Alice'", "VALUES 1");

            assertQuery(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '" + tableName + "'",
                    "VALUES ('id', 'integer'), ('name', 'varchar'), ('unknown_column', 'unknown')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(dataProvider = "fileFormats")
    public void testReadUnknownColumnInsideStruct(String fileFormat)
    {
        String tableName = "test_read_unknown_nested_" + fileFormat.toLowerCase(ENGLISH);
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);
            addUnknownColumn(tableName, "nested", Types.StructType.of(
                    Types.NestedField.optional(1, "value", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "unknown_field", Types.UnknownType.get())));

            assertQuery("SELECT id FROM " + tableName, "VALUES 1");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nested IS NULL", "VALUES 1");
            assertQuery("SELECT nested.unknown_field FROM " + tableName, "VALUES NULL");
            assertEquals(
                    getQueryRunner().execute("SELECT nested FROM " + tableName).getTypes(),
                    ImmutableList.of(RowType.from(ImmutableList.of(
                            RowType.field("value", INTEGER),
                            RowType.field("unknown_field", UNKNOWN)))));
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * A file written before an unknown field was added to a row does not have the field, just like a
     * file written after it was added, so both read back with the field filled in with nulls.
     */
    @Test(dataProvider = "fileFormats")
    public void testReadUnknownFieldAddedToExistingStruct(String fileFormat)
    {
        String tableName = "test_read_unknown_field_added_" + fileFormat.toLowerCase(ENGLISH);
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, nested ROW(value INTEGER)) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, ROW(10))", 1);
            loadTable(tableName).updateSchema()
                    .addColumn("nested", "unknown_field", Types.UnknownType.get())
                    .commit();

            assertQuery("SELECT id, nested.value, nested.unknown_field FROM " + tableName, "VALUES (1, 10, NULL)");
            assertEquals(
                    getQueryRunner().execute("SELECT nested FROM " + tableName).getTypes(),
                    ImmutableList.of(RowType.from(ImmutableList.of(
                            RowType.field("value", INTEGER),
                            RowType.field("unknown_field", UNKNOWN)))));

            assertUpdate("INSERT INTO " + tableName + " VALUES (2, ROW(20, NULL))", 1);
            assertQuery("SELECT id, nested.value, nested.unknown_field FROM " + tableName, "VALUES (1, 10, NULL), (2, 20, NULL)");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * Filter pushdown is only executed by the native worker, so this only checks that a table with
     * an unknown column still plans with pushdown enabled. Planning converts the whole table schema
     * to Hive columns, which is where an unsupported type would fail.
     */
    @Test
    public void testReadUnknownColumnWithFilterPushdownPlans()
    {
        String tableName = "test_read_unknown_column_pushdown";
        Session pushdownFilterEnabled = Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'Alice'), (2, 'Bob')", 2);
            addUnknownColumn(tableName, "unknown_column", Types.UnknownType.get());

            // Filtering on the unknown column itself is not covered here: with pushdown enabled,
            // statistics are read from the snapshot's schema, which predates any column added
            // afterwards, so a filter on such a column fails for every type, not just unknown
            assertQuerySucceeds(pushdownFilterEnabled, "EXPLAIN SELECT * FROM " + tableName + " WHERE id = 1");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(dataProvider = "fileFormats")
    public void testWriteUnknownColumn(String fileFormat)
    {
        String tableName = "test_write_unknown_column_" + fileFormat.toLowerCase(ENGLISH);
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            Types.NestedField field = loadTable(tableName).schema().findField("unknown_column");
            assertEquals(field.type(), Types.UnknownType.get());
            // An unknown column must always be optional
            assertTrue(field.isOptional());

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, NULL)", 1);
            // The unknown column can be left out, like any other optional column
            assertUpdate("INSERT INTO " + tableName + " (id) VALUES 2", 1);
            assertUpdate("INSERT INTO " + tableName + " SELECT 3, NULL", 1);

            assertQuery("SELECT * FROM " + tableName, "VALUES (1, NULL), (2, NULL), (3, NULL)");
            assertQuery("SELECT count(unknown_column) FROM " + tableName, "VALUES 0");
            assertEquals(
                    getQueryRunner().execute("SELECT unknown_column FROM " + tableName).getTypes(),
                    ImmutableList.of(UNKNOWN));

            // The unknown column is not stored in the data files, so the writers report no metrics
            // for its field id, while the columns that are stored do have metrics
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\"", "VALUES 3");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE element_at(value_counts, 2) IS NOT NULL", "VALUES 0");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE element_at(value_counts, 1) IS NULL", "VALUES 0");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(dataProvider = "fileFormats")
    public void testAddUnknownColumn(String fileFormat)
    {
        String tableName = "test_add_unknown_column_" + fileFormat.toLowerCase(ENGLISH);
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN unknown_column UNKNOWN");
            assertEquals(loadTable(tableName).schema().findField("unknown_column").type(), Types.UnknownType.get());

            assertUpdate("INSERT INTO " + tableName + " VALUES (2, NULL)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, NULL), (2, NULL)");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * The unknown type was added in Iceberg format version 3, so declaring such a column in an older
     * table is rejected by Iceberg itself.
     */
    @Test
    public void testUnknownColumnRequiresV3()
    {
        String tableName = "test_unknown_column_requires_v3";
        try {
            assertQueryFails(
                    "CREATE TABLE " + tableName + " (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '2')",
                    "(?s).*Invalid type for unknown_column: unknown is not supported until v3.*");

            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '2')");
            assertQueryFails(
                    "ALTER TABLE " + tableName + " ADD COLUMN unknown_column UNKNOWN",
                    "(?s).*Invalid type for unknown_column: unknown is not supported until v3.*");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * An unknown column only ever holds null, so it cannot be declared NOT NULL.
     */
    @Test
    public void testUnknownColumnCannotBeRequired()
    {
        String tableName = "test_unknown_column_not_null";
        try {
            assertQueryFails(
                    "CREATE TABLE " + tableName + " (id INTEGER, unknown_column UNKNOWN NOT NULL) WITH (\"format-version\" = '3')",
                    ".*Cannot create required field with unknown type: unknown_column.*");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * Only null can be written to an unknown column, so a value of any other type is rejected while
     * the insert is analyzed.
     */
    @Test
    public void testWriteValueToUnknownColumnFails()
    {
        String tableName = "test_write_value_to_unknown_column";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3')");
            assertQueryFails(
                    "INSERT INTO " + tableName + " VALUES (1, 5)",
                    ".*'unknown_column' is of type unknown but expression is of type integer.*");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateTableAsSelectWithUnknownColumn()
    {
        String tableName = "test_ctas_unknown_column";
        String copyName = tableName + "_copy";
        try {
            // A column that is only ever null keeps the unknown type, instead of having to be cast
            assertUpdate("CREATE TABLE " + tableName + " WITH (\"format-version\" = '3') AS SELECT 1 id, NULL unknown_column", 1);
            assertEquals(loadTable(tableName).schema().findField("unknown_column").type(), Types.UnknownType.get());
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, NULL)");

            // Also with column aliases, which are analyzed separately
            assertUpdate("CREATE TABLE " + copyName + " (id, unknown_column) WITH (\"format-version\" = '3') AS SELECT * FROM " + tableName, 1);
            assertEquals(loadTable(copyName).schema().findField("unknown_column").type(), Types.UnknownType.get());
            assertQuery("SELECT * FROM " + copyName, "VALUES (1, NULL)");
        }
        finally {
            dropTable(tableName);
            dropTable(copyName);
        }
    }

    /**
     * Iceberg allows an unknown column to be partitioned by identity and to be sorted on, but not to
     * be bucketed, because no hash is defined for it.
     */
    @Test
    public void testUnknownColumnPartitioningAndSorting()
    {
        String partitionedName = "test_unknown_column_partitioned";
        String sortedName = "test_unknown_column_sorted";
        String bucketedName = "test_unknown_column_bucketed";
        try {
            assertUpdate("CREATE TABLE " + partitionedName + " (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3', partitioning = ARRAY['unknown_column'])");
            assertUpdate("INSERT INTO " + partitionedName + " VALUES (1, NULL), (2, NULL)", 2);
            // Every value is null, so all rows go to the same partition
            assertQuery("SELECT count(*) FROM \"" + partitionedName + "$partitions\"", "VALUES 1");
            assertQuery("SELECT * FROM " + partitionedName, "VALUES (1, NULL), (2, NULL)");

            assertUpdate("CREATE TABLE " + sortedName + " (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3', sorted_by = ARRAY['unknown_column'])");
            assertUpdate("INSERT INTO " + sortedName + " VALUES (1, NULL), (2, NULL)", 2);
            assertQuery("SELECT * FROM " + sortedName, "VALUES (1, NULL), (2, NULL)");

            assertQueryFails(
                    "CREATE TABLE " + bucketedName + " (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3', partitioning = ARRAY['bucket(unknown_column, 2)'])",
                    ".*Invalid source type unknown for transform: bucket\\[2\\].*");
        }
        finally {
            dropTable(partitionedName);
            dropTable(sortedName);
            dropTable(bucketedName);
        }
    }

    /**
     * Iceberg has not implemented promotion from the unknown type to another type, so changing the
     * type of an unknown column is rejected.
     */
    @Test
    public void testPromoteUnknownColumnUnsupported()
    {
        String tableName = "test_promote_unknown_column";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3')");
            assertQueryFails(
                    "ALTER TABLE " + tableName + " ALTER COLUMN unknown_column SET DATA TYPE INTEGER",
                    ".*Cannot change column type: unknown_column: unknown -> int.*");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * An unknown field of a row is left out of the data file, like an unknown column, so the rest of
     * the row is written and read back as usual.
     */
    @Test(dataProvider = "fileFormats")
    public void testWriteUnknownColumnInsideStruct(String fileFormat)
    {
        String tableName = "test_write_unknown_nested_" + fileFormat.toLowerCase(ENGLISH);
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, nested ROW(value INTEGER, unknown_field UNKNOWN)) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, ROW(10, NULL)), (2, NULL)", 2);
            assertUpdate("INSERT INTO " + tableName + " (id) VALUES 3", 1);

            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nested.unknown_field IS NULL", "VALUES 3");
            assertQuery("SELECT id FROM " + tableName + " WHERE nested IS NULL", "VALUES 2, 3");
            assertQuery(withoutParquetDereferencePushdown(), "SELECT id, nested.value FROM " + tableName, "VALUES (1, 10), (2, NULL), (3, NULL)");
            assertEquals(
                    getQueryRunner().execute("SELECT nested FROM " + tableName).getTypes(),
                    ImmutableList.of(RowType.from(ImmutableList.of(
                            RowType.field("value", INTEGER),
                            RowType.field("unknown_field", UNKNOWN)))));

            // The unknown field has no metrics of its own, while the field stored beside it does
            Schema schema = loadTable(tableName).schema();
            int unknownFieldId = schema.findField("nested.unknown_field").fieldId();
            int storedFieldId = schema.findField("nested.value").fieldId();
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE element_at(value_counts, " + unknownFieldId + ") IS NOT NULL", "VALUES 0");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE element_at(value_counts, " + storedFieldId + ") IS NULL", "VALUES 0");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * Deeper nesting works the same way, as long as something is left of the value to store.
     */
    @Test(dataProvider = "fileFormats")
    public void testWriteUnknownColumnInsideNestedTypes(String fileFormat)
    {
        String tableName = "test_write_unknown_deeply_nested_" + fileFormat.toLowerCase(ENGLISH);
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id INTEGER, " +
                    "nested ROW(value INTEGER, inner_row ROW(unknown_field UNKNOWN, other INTEGER)), " +
                    "rows ARRAY(ROW(value INTEGER, unknown_field UNKNOWN)), " +
                    "row_by_name MAP(VARCHAR, ROW(value INTEGER, unknown_field UNKNOWN))) " +
                    "WITH (\"format-version\" = '3', format = '" + fileFormat + "')");

            assertUpdate("INSERT INTO " + tableName + " VALUES (" +
                    "1, " +
                    "ROW(10, ROW(NULL, 20)), " +
                    "ARRAY[ROW(30, NULL), NULL], " +
                    "MAP(ARRAY['a'], ARRAY[ROW(40, NULL)]))", 1);
            assertUpdate("INSERT INTO " + tableName + " (id) VALUES 2", 1);

            assertQuery(
                    withoutParquetDereferencePushdown(),
                    "SELECT id, nested.value, nested.inner_row.other, rows[1].value, row_by_name['a'].value FROM " + tableName + " WHERE id = 1",
                    "VALUES (1, 10, 20, 30, 40)");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nested.inner_row.unknown_field IS NULL", "VALUES 2");
            assertQuery("SELECT cardinality(rows) FROM " + tableName, "VALUES 2, NULL");
            assertQuery("SELECT id FROM " + tableName + " WHERE nested IS NULL", "VALUES 2");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * Parquet dereference pushdown does not read a field of a row whose value is null, for a row with
     * an unknown field and for a row without one alike, so the reads that go through a null row are
     * left to the reader that reads the whole row.
     */
    private Session withoutParquetDereferencePushdown()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, "false")
                .build();
    }

    /**
     * A data file needs something to store for every value, so an unknown type that is the whole of an
     * array element, map key, map value or row is not supported. Iceberg's own writers fail on these
     * as well.
     */
    @Test(dataProvider = "fileFormats")
    public void testWriteUnknownTypeAsWholeNestedValueUnsupported(String fileFormat)
    {
        String suffix = "_" + fileFormat.toLowerCase(ENGLISH);
        String arrayTable = "test_write_unknown_array" + suffix;
        String mapTable = "test_write_unknown_map" + suffix;
        String rowTable = "test_write_unknown_row" + suffix;
        try {
            assertUpdate("CREATE TABLE " + arrayTable + " (id INTEGER, unknowns ARRAY(UNKNOWN)) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            assertQueryFails(
                    "INSERT INTO " + arrayTable + " (id) VALUES 1",
                    ".*Writing to an Iceberg table with an array of type unknown is not supported: unknowns.*");

            assertUpdate("CREATE TABLE " + mapTable + " (id INTEGER, unknown_by_name MAP(VARCHAR, UNKNOWN)) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            assertQueryFails(
                    "INSERT INTO " + mapTable + " (id) VALUES 1",
                    ".*Writing to an Iceberg table with a map of type unknown is not supported: unknown_by_name.*");

            assertUpdate("CREATE TABLE " + rowTable + " (id INTEGER, nested ROW(unknown_field UNKNOWN)) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            assertQueryFails(
                    "INSERT INTO " + rowTable + " (id) VALUES 1",
                    ".*Writing to an Iceberg table with a row whose fields are all of type unknown is not supported: nested.*");
        }
        finally {
            dropTable(arrayTable);
            dropTable(mapTable);
            dropTable(rowTable);
        }
    }

    /**
     * Iceberg allows sorting, grouping and aggregating on an unknown column, so the same operations
     * must work here. Every value is null, so they all collapse to a single null group.
     */
    @Test
    public void testUnknownColumnInSortsAndAggregations()
    {
        String tableName = "test_unknown_column_sort_aggregate";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES 1, 2", 2);
            addUnknownColumn(tableName, "unknown_column", Types.UnknownType.get());

            assertQuery("SELECT id FROM " + tableName + " ORDER BY unknown_column", "VALUES 1, 2");
            assertQuery("SELECT unknown_column, count(*) FROM " + tableName + " GROUP BY 1", "VALUES (NULL, 2)");
            assertQuery("SELECT DISTINCT unknown_column FROM " + tableName, "VALUES NULL");
            assertQuery("SELECT max(unknown_column) FROM " + tableName, "VALUES NULL");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUnknownColumnMetadataTablesAndStatistics()
    {
        String tableName = "test_unknown_column_metadata";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3', partitioning = ARRAY['id'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'Alice'), (2, 'Bob')", 2);
            addUnknownColumn(tableName, "unknown_column", Types.UnknownType.get());

            // An unknown column has no bounds, so it contributes only null min/max to $partitions
            assertQuerySucceeds("SELECT * FROM \"" + tableName + "$partitions\"");
            assertQuerySucceeds("SELECT * FROM \"" + tableName + "$files\"");
            assertQuerySucceeds("SELECT * FROM \"" + tableName + "$manifests\"");
            assertQuerySucceeds("SHOW STATS FOR " + tableName);
            assertQuerySucceeds("ANALYZE " + tableName);
            assertQuerySucceeds("SHOW COLUMNS FROM " + tableName);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRenameAndDropUnknownColumn()
    {
        String tableName = "test_unknown_column_schema_evolution";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);
            addUnknownColumn(tableName, "unknown_column", Types.UnknownType.get());

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN unknown_column TO renamed_unknown");
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, NULL)");

            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN renamed_unknown");
            assertQuery("SELECT * FROM " + tableName, "VALUES 1");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * A data file needs at least one column, so a table of nothing but unknown columns can be
     * created and read but not written to. Iceberg's own writers fail on such a table as well.
     */
    @Test(dataProvider = "fileFormats")
    public void testWriteTableOfOnlyUnknownColumnsUnsupported(String fileFormat)
    {
        String tableName = "test_only_unknown_columns_" + fileFormat.toLowerCase(ENGLISH);
        try {
            assertUpdate("CREATE TABLE " + tableName + " (unknown_column UNKNOWN) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            assertQueryFails(
                    "INSERT INTO " + tableName + " VALUES NULL",
                    ".*Writing to an Iceberg table whose columns are all of type unknown is not supported.*");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");
        }
        finally {
            dropTable(tableName);
        }
    }

    private void addUnknownColumn(String tableName, String columnName, Type type)
    {
        loadTable(tableName).updateSchema()
                .addColumn(columnName, type)
                .commit();
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
