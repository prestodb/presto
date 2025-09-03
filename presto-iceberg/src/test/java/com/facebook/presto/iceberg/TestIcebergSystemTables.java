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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.Session;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestIcebergSystemTables
        extends AbstractTestQueryFramework
{
    private static final int DEFAULT_PRECISION = 5;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        Path dataDirectory = queryRunner.getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false);

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        return queryRunner;
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        super.init();
        assertUpdate("CREATE SCHEMA test_schema");
        assertUpdate("CREATE TABLE test_schema.test_table (_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO test_schema.test_table VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
        assertUpdate("INSERT INTO test_schema.test_table VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table", "VALUES 6");

        assertUpdate("CREATE TABLE test_schema.test_table_v1 (_bigint BIGINT, _date DATE) WITH (\"format-version\" = '1', partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO test_schema.test_table_v1 VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
        assertUpdate("INSERT INTO test_schema.test_table_v1 VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table_v1", "VALUES 6");

        assertUpdate("CREATE TABLE test_schema.test_table_multilevel_partitions (_varchar VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_bigint', '_date'])");
        assertUpdate("INSERT INTO test_schema.test_table_multilevel_partitions VALUES ('a', 0, CAST('2019-09-08' AS DATE)), ('a', 1, CAST('2019-09-08' AS DATE)), ('a', 0, CAST('2019-09-09' AS DATE))", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table_multilevel_partitions", "VALUES 3");

        assertUpdate("CREATE TABLE test_schema.test_table_drop_column (_varchar VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO test_schema.test_table_drop_column VALUES ('a', 0, CAST('2019-09-08' AS DATE)), ('a', 1, CAST('2019-09-09' AS DATE)), ('b', 2, CAST('2019-09-09' AS DATE))", 3);
        assertUpdate("INSERT INTO test_schema.test_table_drop_column VALUES ('c', 3, CAST('2019-09-09' AS DATE)), ('a', 4, CAST('2019-09-10' AS DATE)), ('b', 5, CAST('2019-09-10' AS DATE))", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table_drop_column", "VALUES 6");
        assertUpdate("ALTER TABLE test_schema.test_table_drop_column DROP COLUMN _varchar");

        assertUpdate("CREATE TABLE test_schema.test_table_orc (_bigint BIGINT) WITH (\"format-version\" = '1', \"write.format.default\" = 'ORC')");
        assertUpdate("INSERT INTO test_schema.test_table_orc VALUES (0), (1), (2)", 3);

        assertUpdate("CREATE TABLE test_schema.test_metadata_versions_maintain (_bigint BIGINT)" +
                " WITH (\"write.metadata.previous-versions-max\" = 1, \"write.metadata.delete-after-commit.enabled\" = true)");

        assertUpdate("CREATE TABLE test_schema.test_metrics_max_inferred_column (_bigint BIGINT)" +
                " WITH (\"write.metadata.metrics.max-inferred-column-defaults\" = 16)");
    }

    @Test
    public void testPartitionTable()
    {
        assertQuery("SELECT count(*) FROM test_schema.test_table", "VALUES 6");
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$partitions\"",
                "VALUES ('_date', 'date', '', '', null, null, null)," +
                        "('row_count', 'bigint', '', '', 19L, null, null)," +
                        "('file_count', 'bigint', '', '', 19L, null, null)," +
                        "('total_size', 'bigint', '', '', 19L, null, null)," +
                        "('_bigint', 'row(\"min\" bigint, \"max\" bigint, \"null_count\" bigint)', '', '', null, null, null)");

        MaterializedResult result = computeActual("SELECT * from test_schema.\"test_table$partitions\"");
        assertEquals(result.getRowCount(), 3);

        Map<LocalDate, MaterializedRow> rowsByPartition = result.getMaterializedRows().stream()
                .collect(toImmutableMap(row -> (LocalDate) row.getField(0), Function.identity()));

        // Test if row counts are computed correctly
        assertEquals(rowsByPartition.get(LocalDate.parse("2019-09-08")).getField(1), 1L);
        assertEquals(rowsByPartition.get(LocalDate.parse("2019-09-09")).getField(1), 3L);
        assertEquals(rowsByPartition.get(LocalDate.parse("2019-09-10")).getField(1), 2L);

        // Test if min/max values and null value count are computed correctly.
        assertEquals(
                rowsByPartition.get(LocalDate.parse("2019-09-08")).getField(4),
                new MaterializedRow(DEFAULT_PRECISION, 0L, 0L, 0L).getFields());
        assertEquals(
                rowsByPartition.get(LocalDate.parse("2019-09-09")).getField(4),
                new MaterializedRow(DEFAULT_PRECISION, 1L, 3L, 0L).getFields());
        assertEquals(
                rowsByPartition.get(LocalDate.parse("2019-09-10")).getField(4),
                new MaterializedRow(DEFAULT_PRECISION, 4L, 5L, 0L).getFields());
    }

    @Test
    public void testHistoryTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$history\"",
                "VALUES ('made_current_at', 'timestamp with time zone', '', '', null, null, null)," +
                        "('snapshot_id', 'bigint', '', '', 19l, null, null)," +
                        "('parent_id', 'bigint', '', '', 19l, null, null)," +
                        "('is_current_ancestor', 'boolean', '', '',null , null, null)");

        // Test the number of history entries
        assertQuery("SELECT count(*) FROM test_schema.\"test_table$history\"", "VALUES 2");
    }

    @Test
    public void testSnapshotsTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$snapshots\"",
                "VALUES ('committed_at', 'timestamp with time zone', '', '', null, null, null)," +
                        "('snapshot_id', 'bigint', '', '', 19L, null, null)," +
                        "('parent_id', 'bigint', '', '', 19L, null, null)," +
                        "('operation', 'varchar', '', '', null, null, 2147483647L)," +
                        "('manifest_list', 'varchar', '', '', null, null, 2147483647L)," +
                        "('summary', 'map(varchar, varchar)', '', '', null, null, null)");

        assertQuery("SELECT operation FROM test_schema.\"test_table$snapshots\"", "VALUES 'append', 'append'");
        assertQuery("SELECT summary['total-records'] FROM test_schema.\"test_table$snapshots\"", "VALUES '3', '6'");
    }

    @Test
    public void testManifestsTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$manifests\"",
                "VALUES ('path', 'varchar', '', '', null, null, 2147483647L)," +
                        "('length', 'bigint', '', '', 19L, null, null)," +
                        "('partition_spec_id', 'integer', '', '', 10L, null, null)," +
                        "('added_snapshot_id', 'bigint', '', '', 19L, null, null)," +
                        "('added_data_files_count', 'integer', '', '', 10L, null, null)," +
                        "('existing_data_files_count', 'integer', '', '', 10L, null, null)," +
                        "('deleted_data_files_count', 'integer', '', '', 10L, null, null)," +
                        "('partitions', 'array(row(\"contains_null\" boolean, \"lower_bound\" varchar, \"upper_bound\" varchar))', '', '', null, null, null)");
        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table$manifests\"");

        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table_multilevel_partitions$manifests\"");
    }

    @Test
    public void testFilesTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$files\"",
                "VALUES ('content', 'integer', '', '', 10L, null, null)," +
                        "('file_path', 'varchar', '', '', null, null, 2147483647L)," +
                        "('file_format', 'varchar', '', '', null, null, 2147483647L)," +
                        "('record_count', 'bigint', '', '', 19L, null, null)," +
                        "('file_size_in_bytes', 'bigint', '', '', 19L, null, null)," +
                        "('column_sizes', 'map(integer, bigint)', '', '', null, null, null)," +
                        "('value_counts', 'map(integer, bigint)', '', '', null, null, null)," +
                        "('null_value_counts', 'map(integer, bigint)', '', '', null, null, null)," +
                        "('nan_value_counts', 'map(integer, bigint)', '', '', null, null, null)," +
                        "('lower_bounds', 'map(integer, varchar)', '', '', null, null, null)," +
                        "('upper_bounds', 'map(integer, varchar)', '', '', null, null, null)," +
                        "('key_metadata', 'varbinary', '', '', null, null, null)," +
                        "('split_offsets', 'array(bigint)', '', '', null, null, null)," +
                        "('equality_ids', 'array(integer)', '', '', null, null, null)");
        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table$files\"");
    }

    @Test
    public void testRefsTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$refs\"",
                "VALUES ('name', 'varchar', '', '', null, null, 2147483647L)," +
                        "('type', 'varchar', '', '', null, null, 2147483647L)," +
                        "('snapshot_id', 'bigint', '', '', 19L, null, null)," +
                        "('max_reference_age_in_ms', 'bigint', '', '', 19L, null, null)," +
                        "('min_snapshots_to_keep', 'bigint', '', '', 19L, null, null)," +
                        "('max_snapshot_age_in_ms', 'bigint', '', '', 19L, null, null)");
        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table$refs\"");

        // Check main branch entry
        assertQuery("SELECT count(*) FROM test_schema.\"test_table$refs\"", "VALUES 1");
        assertQuery("SELECT name FROM test_schema.\"test_table$refs\"", "VALUES 'main'");

        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table_multilevel_partitions$refs\"");
    }

    @Test
    public void testSessionPropertiesInManuallyStartedTransaction()
    {
        try {
            assertUpdate("create table test_schema.test_session_properties_table(a int, b varchar)");
            // The default value of table property `delete_mode` is `merge-on-read`
            MaterializedResult materializedRows = getQueryRunner().execute("select * from  test_schema.\"test_session_properties_table$properties\"");
            assertThat(materializedRows)
                    .anySatisfy(row -> assertThat(row)
                            .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.delete.mode", "merge-on-read")));

            // Simulate `set session iceberg.merge_on_read_enabled=false` to disable merge on read mode for iceberg tables in session level
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty(ICEBERG_CATALOG, "merge_on_read_enabled", "false")
                    .build();

            // Simulate `start transaction` to begin a transaction
            TransactionManager transactionManager = getQueryRunner().getTransactionManager();
            TransactionId txnId = transactionManager.beginTransaction(false);
            Session txnSession = session.beginTransactionId(txnId, transactionManager, new AllowAllAccessControl());

            // Query should fail because of the conflicts between session property and table property in table mode validation
            assertQueryFails(txnSession, "select * from test_schema.test_session_properties_table", "merge-on-read table mode not supported yet");
            transactionManager.asyncAbort(txnId);
        }
        finally {
            assertUpdate("drop table if exists test_schema.test_session_properties_table");
        }
    }

    protected void checkTableProperties(String schemaName, String tableName, String deleteMode, String dataWriteLocation)
    {
        checkTableProperties(schemaName, tableName, deleteMode, 10, ImmutableMap.of("write.data.path", dataWriteLocation));
    }

    protected void checkTableProperties(String tableName, String deleteMode)
    {
        checkTableProperties("test_schema", tableName, deleteMode, 9, ImmutableMap.of());
    }

    protected void checkTableProperties(String schemaName, String tableName, String deleteMode, int propertiesCount, Map<String, String> additionalValidateProperties)
    {
        assertQuery(
                String.format("SHOW COLUMNS FROM %s.\"%s$properties\"", schemaName, tableName),
                "VALUES " +
                        "('key', 'varchar', '', '', null, null, 2147483647)," +
                        "('value', 'varchar', '', '', null, null, 2147483647)");
        assertQuery(String.format("SELECT COUNT(*) FROM %s.\"%s$properties\"", schemaName, tableName), "VALUES " + propertiesCount);
        List<MaterializedRow> materializedRows = computeActual(getSession(),
                String.format("SELECT * FROM %s.\"%s$properties\"", schemaName, tableName)).getMaterializedRows();

        assertThat(materializedRows).hasSize(propertiesCount);
        assertThat(materializedRows)
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.delete.mode", deleteMode)))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.update.mode", deleteMode)))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.format.default", "PARQUET")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.parquet.compression-codec", "GZIP")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "commit.retry.num-retries", "4")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.previous-versions-max", "100")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.delete-after-commit.enabled", "false")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.metrics.max-inferred-column-defaults", "100")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, IcebergTableProperties.TARGET_SPLIT_SIZE, Long.toString(DataSize.valueOf("128MB").toBytes()))));

        additionalValidateProperties.entrySet().stream()
                .forEach(entry -> assertThat(materializedRows)
                        .anySatisfy(row -> assertThat(row)
                                .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, entry.getKey(), entry.getValue()))));
    }

    protected void checkORCFormatTableProperties(String tableName, String deleteMode)
    {
        assertQuery(String.format("SHOW COLUMNS FROM test_schema.\"%s$properties\"", tableName),
                "VALUES ('key', 'varchar', '', '', null, null, 2147483647L)," + "('value', 'varchar', '', '', null, null, 2147483647L)");
        assertQuery(String.format("SELECT COUNT(*) FROM test_schema.\"%s$properties\"", tableName), "VALUES 10");
        List<MaterializedRow> materializedRows = computeActual(getSession(),
                String.format("SELECT * FROM test_schema.\"%s$properties\"", tableName)).getMaterializedRows();

        assertThat(materializedRows).hasSize(10);
        assertThat(materializedRows)
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.delete.mode", deleteMode)))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.update.mode", deleteMode)))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.format.default", "ORC")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.orc.compression-codec", "ZLIB")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.parquet.compression-codec", "zstd")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "commit.retry.num-retries", "4")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.previous-versions-max", "100")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.delete-after-commit.enabled", "false")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.metrics.max-inferred-column-defaults", "100")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, IcebergTableProperties.TARGET_SPLIT_SIZE, Long.toString(DataSize.valueOf("128MB").toBytes()))));
    }

    @Test
    public void testPropertiesTable()
    {
        // Test table properties for all supported format versions
        checkTableProperties("test_table_v1", "copy-on-write");
        checkTableProperties("test_table", "merge-on-read");
        checkORCFormatTableProperties("test_table_orc", "copy-on-write");
    }

    @Test
    public void testFilesTableOnDropColumn()
    {
        assertQuery("SELECT sum(record_count) FROM test_schema.\"test_table_drop_column$files\"", "VALUES 6");
    }

    @Test
    public void testAlterTableColumnNotNull()
    {
        String tableName = "test_schema.test_table_add_column";
        assertUpdate("CREATE TABLE " + tableName + " (c1 INTEGER, c2 INTEGER)");
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN c3 INTEGER NOT NULL",
                "This connector does not support add column with non null");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1,1)", 1);
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN c3 INTEGER NOT NULL",
                "This connector does not support add column with non null");
    }

    @Test
    public void testMetadataVersionsMaintainingProperties()
    {
        MaterializedResult materializedRows = getQueryRunner().execute("select * from  test_schema.\"test_metadata_versions_maintain$properties\"");
        assertThat(materializedRows)
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.previous-versions-max", "1")))
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.delete-after-commit.enabled", "true")));
    }

    @Test
    public void testMetricsMaxInferredColumnProperties()
    {
        MaterializedResult materializedRows = getQueryRunner().execute("select * from  test_schema.\"test_metrics_max_inferred_column$properties\"");
        assertThat(materializedRows)
                .anySatisfy(row -> assertThat(row)
                        .isEqualTo(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, "write.metadata.metrics.max-inferred-column-defaults", "16")));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_v1");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_orc");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_multilevel_partitions");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_drop_column");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_add_column");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_metadata_versions_maintain");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_metrics_max_inferred_column");
        assertUpdate("DROP SCHEMA IF EXISTS test_schema");
    }
}
