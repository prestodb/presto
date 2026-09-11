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

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;

public class TestMetadata
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Test
    public void testShowTables()
    {
        String table1 = "metadata_table1";
        String table2 = "metadata_table2";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR) WITH (format = 'PARQUET')", table1));
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, value DOUBLE) WITH (format = 'PARQUET')", table2));
            assertQuery("SHOW TABLES LIKE 'metadata_table%'");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", table2));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", table1));
        }
    }

    @Test
    public void testShowColumns()
    {
        String tableName = "show_columns";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "col_integer INTEGER, " +
                    "col_bigint BIGINT, " +
                    "col_varchar VARCHAR, " +
                    "col_double DOUBLE, " +
                    "col_boolean BOOLEAN, " +
                    "col_decimal DECIMAL(20, 2), " +
                    "col_date DATE, " +
                    "col_timestamp TIMESTAMP" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertQuery(String.format("SHOW COLUMNS FROM %s", tableName));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testDescribeTable()
    {
        String tableName = "describe_table";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "name VARCHAR, " +
                    "amount DOUBLE, " +
                    "active BOOLEAN" +
                    ") WITH (format = 'PARQUET')", tableName));
            assertQuery(String.format("DESCRIBE %s", tableName));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testTableWithComplexTypesMetadata()
    {
        String tableName = "complex_metadata_" + randomTableSuffix();

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "int_array ARRAY(INTEGER), " +
                    "string_map MAP(VARCHAR, INTEGER), " +
                    "person ROW(name VARCHAR, age INTEGER)" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertQuery(String.format("SHOW COLUMNS FROM %s", tableName));
            assertQuery(String.format("DESCRIBE %s", tableName));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testColumnsAfterInsertion()
    {
        String tableName = "columns";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')", tableName), 3);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '3')");
            assertQuery(String.format("SHOW COLUMNS FROM %s", tableName));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testMetadataAfterMultipleInserts()
    {
        String tableName = "multiple_inserts_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, category VARCHAR, value DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'A', 10.0)", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (2, 'B', 20.0)", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (3, 'C', 30.0)", tableName), 1);

            assertQuery(String.format("SHOW COLUMNS FROM %s", tableName));
            assertQuery(String.format("DESCRIBE %s", tableName));
            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '3')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testMetadataWithNullableColumns()
    {
        String tableName = "nullable_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "nullable_varchar VARCHAR, " +
                    "nullable_double DOUBLE" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'value1', 1.0), (2, NULL, 2.0), (3, 'value3', NULL)", tableName), 3);

            assertQuery(String.format("SHOW COLUMNS FROM %s", tableName));
            assertQuery(String.format("DESCRIBE %s", tableName));
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE nullable_varchar IS NULL", tableName), "VALUES (BIGINT '1')");
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE nullable_double IS NULL", tableName), "VALUES (BIGINT '1')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testMetadataWithAllPrimitiveTypes()
    {
        String tableName = "all_types_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "col_boolean BOOLEAN, " +
                    "col_integer INTEGER, " +
                    "col_bigint BIGINT, " +
                    "col_real REAL, " +
                    "col_double DOUBLE, " +
                    "col_decimal DECIMAL(10, 2), " +
                    "col_varchar VARCHAR, " +
                    "col_varbinary VARBINARY, " +
                    "col_date DATE, " +
                    "col_timestamp TIMESTAMP" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertQuery(String.format("SHOW COLUMNS FROM %s", tableName));
            assertQuery(String.format("DESCRIBE %s", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "true, 123, 456789, REAL '1.23', 4.56, DECIMAL '78.90', 'text', X'ABCD', DATE '2024-01-01', TIMESTAMP '2024-01-01 12:00:00')", tableName), 1);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '1')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testMetadataAfterTableRecreation()
    {
        String tableName = "recreate_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'first')", tableName), 1);

            // Drop and recreate with different schema
            assertUpdate(String.format("DROP TABLE %s", tableName));
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, name VARCHAR, value DOUBLE) WITH (format = 'PARQUET')", tableName));

            // Verify new schema
            assertQuery(String.format("SHOW COLUMNS FROM %s", tableName));
            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '0')");

            // Insert data with new schema
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'test', 1.5)", tableName), 1);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '1')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testHiddenMetadataColumnPath()
    {
        String tableName = "hidden_path";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, name VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName), 2);

            // Query the $path hidden column
            assertQuery(String.format("SELECT \"$path\", id, name FROM %s ORDER BY id", tableName));

            // Verify $path column is not null
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE \"$path\" IS NOT NULL", tableName), "VALUES (BIGINT '2')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testHiddenMetadataColumnDataSequenceNumber()
    {
        String tableName = "hidden_sequence";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, value DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 10.0), (2, 20.0)", tableName), 2);

            // Query the $data_sequence_number hidden column
            assertQuery(String.format("SELECT \"$data_sequence_number\", id FROM %s ORDER BY id", tableName));

            // Verify $data_sequence_number column is not null
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE \"$data_sequence_number\" IS NOT NULL", tableName), "VALUES (BIGINT '2')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testHiddenMetadataColumnsWithComplexQuery()
    {
        String tableName = "hidden_complex";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, category VARCHAR, amount DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0)", tableName), 3);

            // Query multiple hidden columns together with regular columns
            assertQuery(String.format("SELECT \"$path\", \"$data_sequence_number\", id, category FROM %s WHERE category = 'A' ORDER BY id", tableName));

            // Verify both hidden columns are accessible
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE \"$path\" IS NOT NULL AND \"$data_sequence_number\" IS NOT NULL", tableName),
                    "VALUES (BIGINT '3')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testHiddenMetadataColumnsAfterMultipleInserts()
    {
        String tableName = "hidden_multi_insert";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'first')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (2, 'second')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (3, 'third')", tableName), 1);

            assertQuery(String.format("SELECT \"$path\", \"$data_sequence_number\", id FROM %s ORDER BY id", tableName));
            // Verify all rows have hidden metadata
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE \"$path\" IS NOT NULL", tableName), "VALUES (BIGINT '3')");
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE \"$data_sequence_number\" IS NOT NULL", tableName), "VALUES (BIGINT '3')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testHiddenMetadataColumnsWithAggregation()
    {
        String tableName = "hidden_aggregation";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, category VARCHAR, value DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'A', 10.0), (2, 'B', 20.0), (3, 'A', 30.0), (4, 'B', 40.0)", tableName), 4);

            // Use hidden columns in aggregation queries
            assertQuery(String.format("SELECT category, COUNT(\"$path\") as file_count FROM %s GROUP BY category ORDER BY category", tableName),
                    "VALUES ('A', BIGINT '2'), ('B', BIGINT '2')");
            assertQuery(String.format("SELECT COUNT(DISTINCT \"$path\") FROM %s", tableName));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testPropertiesMetadataTable()
    {
        String tableName = "properties_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'test')", tableName), 1);

            assertQuery(String.format("SELECT * FROM \"%s$properties\"", tableName));
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$properties\"", tableName), "VALUES (true)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testHistoryMetadataTable()
    {
        String tableName = "history_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, value DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 10.0)", tableName), 1);

            assertQuery(String.format("SELECT snapshot_id, is_current_ancestor FROM \"%s$history\"", tableName));
            // Verify history table has at least one entry
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$history\"", tableName), "VALUES (true)");
            // Verify snapshot_id is not null
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$history\" WHERE snapshot_id IS NOT NULL", tableName));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSnapshotsMetadataTable()
    {
        String tableName = "snapshots_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, name VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'first')", tableName), 1);

            assertQuery(String.format("SELECT snapshot_id, operation FROM \"%s$snapshots\"", tableName));
            // Verify snapshots table has at least one entry
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$snapshots\"", tableName), "VALUES (true)");

            // Verify snapshot_id is not null
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$snapshots\" WHERE snapshot_id IS NOT NULL", tableName));

            // Insert more data and verify new snapshot is created
            assertUpdate(String.format("INSERT INTO %s VALUES (2, 'second')", tableName), 1);
            assertQuery(String.format("SELECT COUNT(*) >= 2 FROM \"%s$snapshots\"", tableName), "VALUES (true)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testManifestsMetadataTable()
    {
        String tableName = "manifests_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'test')", tableName), 1);

            assertQuery(String.format("SELECT path, partition_spec_id, added_snapshot_id FROM \"%s$manifests\"", tableName));

            // Verify manifests table has at least one entry
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$manifests\"", tableName), "VALUES (true)");

            // Verify added_snapshot_id is not null
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$manifests\" WHERE added_snapshot_id IS NOT NULL", tableName));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testPartitionsMetadataTable()
    {
        String tableName = "partitions_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, category VARCHAR, value DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'A', 10.0), (2, 'B', 20.0)", tableName), 2);

            assertQuery(String.format("SELECT row_count, file_count, total_size FROM \"%s$partitions\"", tableName));

            // Verify partitions table has data
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$partitions\"", tableName), "VALUES (true)");

            // Verify row_count matches inserted rows
            assertQuery(String.format("SELECT SUM(row_count) FROM \"%s$partitions\"", tableName), "VALUES (BIGINT '2')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testFilesMetadataTable()
    {
        String tableName = "files_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, name VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName), 2);

            assertQuery(String.format("SELECT content, file_format, record_count, file_size_in_bytes FROM \"%s$files\"", tableName));

            // Verify files table has at least one entry
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$files\"", tableName), "VALUES (true)");

            // Verify file_format is PARQUET
            assertQuery(String.format("SELECT DISTINCT file_format FROM \"%s$files\"", tableName), "VALUES ('PARQUET')");

            // Verify file_path is not null
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$files\" WHERE file_path IS NOT NULL", tableName));

            // Verify total record count matches inserted rows
            assertQuery(String.format("SELECT SUM(record_count) FROM \"%s$files\"", tableName), "VALUES (BIGINT '2')");

            // Verify file_size_in_bytes is positive
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$files\" WHERE file_size_in_bytes > 0", tableName));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testRefsMetadataTable()
    {
        String tableName = "refs_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'test')", tableName), 1);

            assertQuery(String.format("SELECT name, type, snapshot_id FROM \"%s$refs\"", tableName));
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$refs\"", tableName), "VALUES (true)");

            // Verify main branch exists
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$refs\" WHERE name = 'main'", tableName));

            // Verify snapshot_id is not null for main branch
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$refs\" WHERE name = 'main' AND snapshot_id IS NOT NULL", tableName));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testWithMultipleSnapshots()
    {
        String tableName = "multi_snapshot";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, value DOUBLE) WITH (format = 'PARQUET')", tableName));

            // Create multiple snapshots with multiple inserts
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 10.0)", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (2, 20.0)", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (3, 30.0)", tableName), 1);

            assertQuery(String.format("SELECT COUNT(*) >= 3 FROM \"%s$history\"", tableName), "VALUES (true)");
            assertQuery(String.format("SELECT COUNT(*) >= 3 FROM \"%s$snapshots\"", tableName), "VALUES (true)");
            assertQuery(String.format("SELECT COUNT(*) >= 3 FROM \"%s$files\"", tableName), "VALUES (true)");
            assertQuery(String.format("SELECT SUM(record_count) FROM \"%s$files\"", tableName), "VALUES (BIGINT '3')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testWithComplexQueries()
    {
        String tableName = "complex_metadata_" + randomTableSuffix();

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, category VARCHAR, amount DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'A', 100.0), (2, 'B', 200.0)", tableName), 2);

            // Aggregate data from $files
            assertQuery(String.format("SELECT file_format, COUNT(*) as file_count FROM \"%s$files\" GROUP BY file_format", tableName));

            // Query $manifests with filtering
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$manifests\" WHERE partition_spec_id >= 0", tableName));

            // Query $snapshots with filtering
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$snapshots\" WHERE snapshot_id IS NOT NULL", tableName));

            // Verify file format from $files
            assertQuery(String.format("SELECT DISTINCT file_format FROM \"%s$files\"", tableName), "VALUES ('PARQUET')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testAllMetadata()
    {
        String tableName = "all_metadata";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'test')", tableName), 1);

            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$properties\"", tableName), "VALUES (true)");
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$history\"", tableName), "VALUES (true)");
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$snapshots\"", tableName), "VALUES (true)");
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$manifests\"", tableName), "VALUES (true)");
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$partitions\"", tableName), "VALUES (true)");
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$files\"", tableName), "VALUES (true)");
            assertQuery(String.format("SELECT COUNT(*) > 0 FROM \"%s$refs\"", tableName), "VALUES (true)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testFilesMetadata()
    {
        String tableName = "files_schema";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, name VARCHAR, value DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", tableName), 2);

            assertQuery(String.format("SELECT content FROM \"%s$files\"", tableName));
            assertQuery(String.format("SELECT file_path FROM \"%s$files\"", tableName));
            assertQuery(String.format("SELECT file_format FROM \"%s$files\"", tableName));
            assertQuery(String.format("SELECT record_count FROM \"%s$files\"", tableName));
            assertQuery(String.format("SELECT file_size_in_bytes FROM \"%s$files\"", tableName));
            assertQuery(String.format("SELECT DISTINCT content FROM \"%s$files\"", tableName), "VALUES (0)");
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$files\" WHERE column_sizes IS NOT NULL", tableName));
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$files\" WHERE value_counts IS NOT NULL", tableName));
            assertQuery(String.format("SELECT COUNT(*) FROM \"%s$files\" WHERE null_value_counts IS NOT NULL", tableName));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }
}
