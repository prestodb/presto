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
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
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

        Path catalogDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").resolve("catalog");

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDir.toFile().toURI().toString())
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        return queryRunner;
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate("CREATE SCHEMA test_schema");
        assertUpdate("CREATE TABLE test_schema.test_table (_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO test_schema.test_table VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
        assertUpdate("INSERT INTO test_schema.test_table VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table", "VALUES 6");

        assertUpdate("CREATE TABLE test_schema.test_table_multilevel_partitions (_varchar VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_bigint', '_date'])");
        assertUpdate("INSERT INTO test_schema.test_table_multilevel_partitions VALUES ('a', 0, CAST('2019-09-08' AS DATE)), ('a', 1, CAST('2019-09-08' AS DATE)), ('a', 0, CAST('2019-09-09' AS DATE))", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table_multilevel_partitions", "VALUES 3");
    }

    @Test
    public void testPartitionTable()
    {
        assertQuery("SELECT count(*) FROM test_schema.test_table", "VALUES 6");
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$partitions\"",
                "VALUES ('_date', 'date', '', '')," +
                        "('row_count', 'bigint', '', '')," +
                        "('file_count', 'bigint', '', '')," +
                        "('total_size', 'bigint', '', '')," +
                        "('_bigint', 'row(\"min\" bigint, \"max\" bigint, \"null_count\" bigint)', '', '')");

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
                "VALUES ('made_current_at', 'timestamp with time zone', '', '')," +
                        "('snapshot_id', 'bigint', '', '')," +
                        "('parent_id', 'bigint', '', '')," +
                        "('is_current_ancestor', 'boolean', '', '')");

        // Test the number of history entries
        assertQuery("SELECT count(*) FROM test_schema.\"test_table$history\"", "VALUES 3");
    }

    @Test
    public void testSnapshotsTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$snapshots\"",
                "VALUES ('committed_at', 'timestamp with time zone', '', '')," +
                        "('snapshot_id', 'bigint', '', '')," +
                        "('parent_id', 'bigint', '', '')," +
                        "('operation', 'varchar', '', '')," +
                        "('manifest_list', 'varchar', '', '')," +
                        "('summary', 'map(varchar, varchar)', '', '')");

        assertQuery("SELECT operation FROM test_schema.\"test_table$snapshots\"", "VALUES 'append', 'append', 'append'");
        assertQuery("SELECT summary['total-records'] FROM test_schema.\"test_table$snapshots\"", "VALUES '0', '3', '6'");
    }

    @Test
    public void testManifestsTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$manifests\"",
                "VALUES ('path', 'varchar', '', '')," +
                        "('length', 'bigint', '', '')," +
                        "('partition_spec_id', 'integer', '', '')," +
                        "('added_snapshot_id', 'bigint', '', '')," +
                        "('added_data_files_count', 'integer', '', '')," +
                        "('existing_data_files_count', 'integer', '', '')," +
                        "('deleted_data_files_count', 'integer', '', '')," +
                        "('partitions', 'array(row(\"contains_null\" boolean, \"lower_bound\" varchar, \"upper_bound\" varchar))', '', '')");
        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table$manifests\"");

        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table_multilevel_partitions$manifests\"");
    }

    @Test
    public void testFilesTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$files\"",
                "VALUES ('file_path', 'varchar', '', '')," +
                        "('file_format', 'varchar', '', '')," +
                        "('record_count', 'bigint', '', '')," +
                        "('file_size_in_bytes', 'bigint', '', '')," +
                        "('column_sizes', 'map(integer, bigint)', '', '')," +
                        "('value_counts', 'map(integer, bigint)', '', '')," +
                        "('null_value_counts', 'map(integer, bigint)', '', '')," +
                        "('lower_bounds', 'map(integer, varchar)', '', '')," +
                        "('upper_bounds', 'map(integer, varchar)', '', '')," +
                        "('key_metadata', 'varbinary', '', '')," +
                        "('split_offsets', 'array(bigint)', '', '')");
        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table$files\"");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_multilevel_partitions");
        assertUpdate("DROP SCHEMA IF EXISTS test_schema");
    }
}
