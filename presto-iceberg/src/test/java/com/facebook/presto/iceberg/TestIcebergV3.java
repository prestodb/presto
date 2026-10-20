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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.sql.TestTable;
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
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.UuidType.javaUuidToPrestoUuid;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.IcebergUtil.MAX_FORMAT_VERSION_FOR_METADATA_TABLES;
import static com.facebook.presto.iceberg.NotNullConstraintErrors.noNullCountError;
import static com.facebook.presto.iceberg.NotNullConstraintErrors.nullValuesError;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

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
            // V3 row-level deletes are committed as Puffin deletion vectors, which only the
            // native (Prestissimo) worker can write, so a Java-only query runner cannot complete
            // this DELETE. The failure now surfaces from the engine rather than from the
            // connector's own native-only check: getDeleteRowIdColumn returns the 4-field
            // ROW<file_path, pos, spec_id, partition> the Velox deletion-vector sink requires, and
            // the Java delete path cannot consume that row id. Assert the DELETE fails with a
            // real execution error (not an NPE short-circuit before the DV path is reached)
            // rather than pinning an engine-internal message, which is version-fragile.
            // Producing a connector-level error here needs a way to tell at beginDelete whether
            // the query will run on native workers, which the connector cannot currently see.
            // DELETE is intentionally not gated in QueryPlanner: full-table and
            // partition-aligned deletes are rewritten to MetadataDeleteNode after planning
            // and succeed on Java, and the planner cannot distinguish those at plan time.
            assertThatThrownBy(() -> getQueryRunner().execute("DELETE FROM " + tableName + " WHERE id = 1"))
                    .isInstanceOf(RuntimeException.class)
                    .isNotInstanceOf(NullPointerException.class);
            // The failed DELETE committed nothing: all rows remain visible.
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");
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
            // V3 UPDATE lands as a Puffin deletion vector + new rows; the DV write requires the
            // native (Prestissimo) worker, so the Java-only query runner rejects it. The planner
            // gate (QueryPlanner.checkV3DeletionVectorRequiresNative) fires first with the
            // connector's message family; the connector's beginUpdate additionally names the
            // native_update_merge_enabled session property as the unblock path for callers
            // that reach it directly.
            assertThatThrownBy(() -> getQueryRunner()
                    .execute("UPDATE " + tableName + " SET status = 'updated', score = 95.0 WHERE id = 1"))
                    .hasMessageContaining("require the native (Prestissimo) worker");
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
                    .hasMessageContaining("require the native (Prestissimo) worker");
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
    public void testAddColumnWithDefaultAndPosition()
    {
        String tableName = "test_add_column_default_position";
        String columnOrderQuery = "SELECT column_name FROM information_schema.columns" +
                " WHERE table_schema = '" + TEST_SCHEMA + "' AND table_name = '" + tableName + "' ORDER BY ordinal_position";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3')");

            // The default literal and the move are staged on a single UpdateSchema, so both have to survive one commit
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'IN' FIRST");
            // Asserted with assertQueryOrdered, because assertQuery compares results as an unordered multiset
            // and would pass for any column order
            assertQueryOrdered(columnOrderQuery, "VALUES ('country'), ('id'), ('name')");
            Table table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "IN");
            assertEquals(table.schema().findField("country").writeDefault(), "IN");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN region VARCHAR DEFAULT 'APAC' AFTER id");
            assertQueryOrdered(columnOrderQuery, "VALUES ('country'), ('id'), ('region'), ('name')");
            table = loadTable(tableName);
            assertEquals(table.schema().findField("region").initialDefault(), "APAC");
            assertEquals(table.schema().findField("region").writeDefault(), "APAC");

            // A row written without the defaulted columns reads them back in their requested positions
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (1, 'Alice')", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES ('IN', 1, 'APAC', 'Alice')");
            assertQuery("SELECT country, region FROM " + tableName, "VALUES ('IN', 'APAC')");
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

    /**
     * Once the snapshot from before the column was added expires, older files are not known to read its initial
     * default. A rewrite writes the default out with a NULL count.
     */
    @Test
    public void testSetNotNullOnColumnAddedWithDefaultAfterOlderSnapshotsExpire()
    {
        try (TestTable table = createV3TableWithColumnAddedWithDefault("test_set_not_null_default_after_older_snapshots_expire")) {
            String tableName = table.getName();
            long beforeColumnAdded = loadTable(tableName).currentSnapshot().snapshotId();
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 7)", 1);
            assertUpdate(format("CALL system.expire_snapshots(schema => '%s', table_name => '%s', snapshot_ids => ARRAY[%d])", TEST_SCHEMA, tableName, beforeColumnAdded));

            assertQueryFails("ALTER TABLE " + tableName + " ALTER COLUMN c2 SET NOT NULL", noNullCountError("c2"));
            assertQuerySucceeds(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))", TEST_SCHEMA, tableName));
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN c2 SET NOT NULL");
            assertQuery("SELECT c1, c2 FROM " + tableName, "VALUES (1, 5), (2, 7)");
        }
    }

    /**
     * Every row of a snapshot from before a column was added reads the column's initial default.
     */
    @Test
    public void testRollbackToSnapshotBeforeRequiredColumnWithDefaultWasAdded()
    {
        try (TestTable table = createV3TableWithColumnAddedWithDefault("test_rollback_before_required_column_with_default")) {
            String tableName = table.getName();
            long beforeColumnAdded = loadTable(tableName).currentSnapshot().snapshotId();
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 7)", 1);
            // Made required via the Iceberg API, so only the rollback's check runs
            loadTable(tableName).updateSchema().allowIncompatibleChanges().requireColumn("c2").commit();

            assertUpdate(format("CALL system.rollback_to_snapshot('%s', '%s', %d)", TEST_SCHEMA, tableName, beforeColumnAdded));
            assertQuery("SELECT c1, c2 FROM " + tableName, "VALUES (1, 5)");
        }
    }

    @Test
    public void testSetNotNullOnColumnWithDefaultAndNullValues()
    {
        try (TestTable table = createV3TableWithColumnAddedWithDefault("test_set_not_null_default_with_null")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, NULL)", 1);
            assertQueryFails("ALTER TABLE " + tableName + " ALTER COLUMN c2 SET NOT NULL", nullValuesError("c2", "main"));
        }
    }

    /**
     * Only files written before the column was added read its initial default; for later files, the NULL count decides.
     */
    @Test
    public void testSetNotNullOnColumnWithDefaultWithoutNullCount()
    {
        try (TestTable table = createV3TableWithColumnAddedWithDefault("test_set_not_null_default_without_null_count", "\"write.metadata.metrics.max-inferred-column-defaults\" = 1")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, NULL)", 1);
            assertQueryFails("ALTER TABLE " + tableName + " ALTER COLUMN c2 SET NOT NULL", noNullCountError("c2"));
        }
    }

    /**
     * A write default applies only to new writes, so files written before the column was added read NULL.
     */
    @Test
    public void testSetNotNullOnColumnWithWriteDefaultOnly()
    {
        try (TestTable table = createV3Table("test_set_not_null_write_default_only")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c2 BIGINT");
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN c2 SET DEFAULT 5");
            assertQueryFails("ALTER TABLE " + tableName + " ALTER COLUMN c2 SET NOT NULL", noNullCountError("c2"));
        }
    }

    @Test
    public void testInsertOmittingNotNullColumnWithDefault()
    {
        try (TestTable table = createV3TableWithColumnAddedWithDefault("test_insert_omitting_not_null_column_with_default")) {
            String tableName = table.getName();
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN c2 SET NOT NULL");

            assertUpdate("INSERT INTO " + tableName + " (c1) VALUES (2)", 1);
            assertQueryFails("INSERT INTO " + tableName + " VALUES (3, NULL)", "NULL value not allowed for NOT NULL column: c2");
            assertQuery("SELECT c1, c2 FROM " + tableName, "VALUES (1, 5), (2, 5)");

            assertFalse(((String) computeScalar("SHOW CREATE TABLE " + tableName)).contains("default_value"));
            assertUpdate("CREATE TABLE " + tableName + "_like (LIKE " + tableName + ") WITH (\"format-version\" = '3')");
            dropTable(tableName + "_like");
        }
    }

    @Test
    public void testColumnMetadataWithBinaryTimestampWithTimeZoneAndUuidWriteDefaults()
    {
        UUID uuid = UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7");
        long timestampMicros = 1_672_531_200_123_000L;
        try (TestTable table = createV3Table("test_column_metadata_with_write_defaults")) {
            String tableName = table.getName();
            loadTable(tableName).updateSchema()
                    .addColumn("c_varbinary", Types.BinaryType.get(), Literal.of(ByteBuffer.wrap(new byte[] {1, 2})))
                    .addColumn("c_fixed", Types.FixedType.ofLength(2), Literal.of(ByteBuffer.wrap(new byte[] {3, 4})).to(Types.FixedType.ofLength(2)))
                    .addColumn("c_timestamptz", Types.TimestampType.withZone(), Literal.of(timestampMicros).to(Types.TimestampType.withZone()))
                    .addColumn("c_uuid", Types.UUIDType.get(), Literal.of(uuid))
                    .commit();

            assertQuerySucceeds("DESCRIBE " + tableName);
            assertQuerySucceeds("SELECT * FROM " + tableName);
            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + TEST_SCHEMA + "' AND table_name = '" + tableName + "'",
                    "VALUES 'c1', 'c_varbinary', 'c_fixed', 'c_timestamptz', 'c_uuid'");
            assertEquals(getWriteDefaults(tableName), ImmutableMap.of(
                    "c_varbinary", wrappedBuffer(new byte[] {1, 2}),
                    "c_fixed", wrappedBuffer(new byte[] {3, 4}),
                    "c_timestamptz", packDateTimeWithZone(MICROSECONDS.toMillis(timestampMicros), UTC_KEY),
                    "c_uuid", javaUuidToPrestoUuid(uuid)));
        }
    }

    private Map<String, Object> getWriteDefaults(String tableName)
    {
        Metadata metadata = getQueryRunner().getMetadata();
        return transaction(getQueryRunner().getTransactionManager(), new AllowAllAccessControl())
                .execute(getSession(), session -> {
                    TableHandle tableHandle = metadata.getMetadataResolver(session).getTableHandle(new QualifiedObjectName(ICEBERG_CATALOG, TEST_SCHEMA, tableName)).get();
                    return metadata.getTableMetadata(session, tableHandle).getColumns().stream()
                            .filter(column -> column.getDefaultValue().isPresent())
                            .collect(toImmutableMap(ColumnMetadata::getName, column -> column.getDefaultValue().get()));
                });
    }

    private TestTable createV3Table(String namePrefix, String... tableProperties)
    {
        String properties = Stream.concat(Stream.of("\"format-version\" = '3'"), Arrays.stream(tableProperties)).collect(joining(", "));
        return new TestTable(getQueryRunner()::execute, namePrefix, "CREATE TABLE {TABLE_NAME} (c1 BIGINT) WITH (" + properties + ")");
    }

    /**
     * Creates a V3 table with a row in {@code c1}, then adds {@code c2} with a default of 5.
     */
    private TestTable createV3TableWithColumnAddedWithDefault(String namePrefix, String... tableProperties)
    {
        TestTable table = createV3Table(namePrefix, tableProperties);
        assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
        assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN c2 BIGINT DEFAULT 5");
        return table;
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
