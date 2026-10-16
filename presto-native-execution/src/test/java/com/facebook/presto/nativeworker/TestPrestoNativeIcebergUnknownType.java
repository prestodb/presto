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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;

public class TestPrestoNativeIcebergUnknownType
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected void createTables()
    {
        QueryRunner javaRunner = (QueryRunner) getExpectedQueryRunner();

        javaRunner.execute("DROP TABLE IF EXISTS test_native_unknown_read");
        javaRunner.execute("CREATE TABLE test_native_unknown_read (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3')");
        javaRunner.execute("INSERT INTO test_native_unknown_read VALUES (1, 'Alice'), (2, 'Bob')");
        javaRunner.execute("ALTER TABLE test_native_unknown_read ADD COLUMN unknown_column UNKNOWN");

        javaRunner.execute("DROP TABLE IF EXISTS test_native_unknown_predicates");
        javaRunner.execute("CREATE TABLE test_native_unknown_predicates (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3')");
        javaRunner.execute("INSERT INTO test_native_unknown_predicates VALUES (1, NULL), (2, NULL)");

        javaRunner.execute("DROP TABLE IF EXISTS test_native_unknown_scalar");
        javaRunner.execute("CREATE TABLE test_native_unknown_scalar (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3')");
        javaRunner.execute("INSERT INTO test_native_unknown_scalar VALUES (1, NULL)");

        javaRunner.execute("DROP TABLE IF EXISTS test_native_unknown_aggregations");
        javaRunner.execute("CREATE TABLE test_native_unknown_aggregations (id INTEGER) WITH (\"format-version\" = '3')");
        javaRunner.execute("INSERT INTO test_native_unknown_aggregations VALUES 1, 2");
        javaRunner.execute("ALTER TABLE test_native_unknown_aggregations ADD COLUMN unknown_column UNKNOWN");

        javaRunner.execute("DROP TABLE IF EXISTS test_native_unknown_partitioned");
        javaRunner.execute("CREATE TABLE test_native_unknown_partitioned (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3', partitioning = ARRAY['unknown_column'])");
        javaRunner.execute("INSERT INTO test_native_unknown_partitioned VALUES (1, NULL), (2, NULL)");

        javaRunner.execute("DROP TABLE IF EXISTS test_native_unknown_metadata");
        javaRunner.execute("CREATE TABLE test_native_unknown_metadata (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3', partitioning = ARRAY['id'])");
        javaRunner.execute("INSERT INTO test_native_unknown_metadata VALUES (1, 'Alice'), (2, 'Bob')");
        javaRunner.execute("ALTER TABLE test_native_unknown_metadata ADD COLUMN unknown_column UNKNOWN");
    }

    @Test
    public void testReadUnknownColumn()
    {
        // An unknown column is never stored in a data file; the native worker must synthesise NULLs.
        assertQuery("SELECT * FROM test_native_unknown_read", "VALUES (1, 'Alice', NULL), (2, 'Bob', NULL)");
        assertQuery("SELECT unknown_column FROM test_native_unknown_read", "VALUES NULL, NULL");
        assertQuery("SELECT count(*) FROM test_native_unknown_read", "VALUES 2");
        assertQuery("SELECT count(unknown_column) FROM test_native_unknown_read", "VALUES 0");
        assertQuery("SELECT id FROM test_native_unknown_read WHERE name = 'Alice'", "VALUES 1");
    }

    @Test
    public void testReadUnknownColumnWithFilterPushdown()
    {
        // Filter pushdown is native-only; planning must succeed even when the schema contains an unknown column.
        Session pushdown = Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
        assertQuerySucceeds(pushdown, "EXPLAIN SELECT * FROM test_native_unknown_read WHERE id = 1");
        assertQuery(pushdown, "SELECT id FROM test_native_unknown_read WHERE id = 1", "VALUES 1");
    }

    @Test
    public void testWriteUnknownColumn()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_native_unknown_write");
        try {
            assertQuerySucceeds("CREATE TABLE test_native_unknown_write (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO test_native_unknown_write VALUES (1, NULL)");
            assertQuerySucceeds("INSERT INTO test_native_unknown_write (id) VALUES 2");
            assertQuerySucceeds("INSERT INTO test_native_unknown_write SELECT 3, NULL");
            assertQuery("SELECT * FROM test_native_unknown_write", "VALUES (1, NULL), (2, NULL), (3, NULL)");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS test_native_unknown_write");
        }
    }

    @Test
    public void testAddUnknownColumn()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_native_unknown_add_column");
        try {
            assertQuerySucceeds("CREATE TABLE test_native_unknown_add_column (id INTEGER) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO test_native_unknown_add_column VALUES 1");
            assertQuerySucceeds("ALTER TABLE test_native_unknown_add_column ADD COLUMN unknown_column UNKNOWN");
            assertQuerySucceeds("INSERT INTO test_native_unknown_add_column VALUES (2, NULL)");
            assertQuery("SELECT * FROM test_native_unknown_add_column", "VALUES (1, NULL), (2, NULL)");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS test_native_unknown_add_column");
        }
    }

    @Test
    public void testUnknownColumnPredicates()
    {
        // unknown is always null, so IS NULL matches every row and IS NOT NULL matches none
        assertQuery("SELECT id FROM test_native_unknown_predicates WHERE unknown_column IS NULL ORDER BY id", "VALUES 1, 2");
        assertQuery("SELECT count(*) FROM test_native_unknown_predicates WHERE unknown_column IS NOT NULL", "VALUES 0");
        assertQuery("SELECT count(*) FROM test_native_unknown_predicates WHERE unknown_column = unknown_column", "VALUES 0");
    }

    @Test
    public void testUnknownColumnScalarFunctions()
    {
        assertQuery("SELECT coalesce(unknown_column, 42) FROM test_native_unknown_scalar", "VALUES 42");
        assertQuery("SELECT try_cast(unknown_column AS INTEGER) FROM test_native_unknown_scalar", "VALUES CAST(NULL AS INTEGER)");
        assertQuery("SELECT unknown_column IS DISTINCT FROM NULL FROM test_native_unknown_scalar", "VALUES false");
    }

    @Test
    public void testUnknownColumnInAggregations()
    {
        assertQuery("SELECT id FROM test_native_unknown_aggregations ORDER BY unknown_column", "VALUES 1, 2");
        assertQuery("SELECT unknown_column, count(*) FROM test_native_unknown_aggregations GROUP BY 1", "VALUES (NULL, 2)");
        assertQuery("SELECT DISTINCT unknown_column FROM test_native_unknown_aggregations", "VALUES NULL");
        assertQuery("SELECT max(unknown_column) FROM test_native_unknown_aggregations", "VALUES NULL");
    }

    @Test
    public void testUnknownColumnPartitioningAndSorting()
    {
        // Every value is null so all rows land in the same identity partition
        assertQuery("SELECT count(*) FROM \"test_native_unknown_partitioned$partitions\"", "VALUES 1");
        assertQuery("SELECT * FROM test_native_unknown_partitioned", "VALUES (1, NULL), (2, NULL)");
    }

    @Test
    public void testUnknownColumnWithSort()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_native_unknown_with_sort");
        try {
            assertQuerySucceeds("CREATE TABLE test_native_unknown_with_sort (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3', sorted_by = ARRAY['id'])");
            assertQuerySucceeds("INSERT INTO test_native_unknown_with_sort VALUES (3, NULL), (1, NULL), (2, NULL)");
            assertQuery("SELECT id FROM test_native_unknown_with_sort ORDER BY id", "VALUES 1, 2, 3");
            assertQuery("SELECT unknown_column FROM test_native_unknown_with_sort", "VALUES NULL, NULL, NULL");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS test_native_unknown_with_sort");
        }
    }

    @Test
    public void testCtasWithUnknownColumn()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_native_ctas_unknown");
        try {
            assertQuerySucceeds("CREATE TABLE test_native_ctas_unknown WITH (\"format-version\" = '3') AS SELECT 1 AS id, NULL AS marker");
            assertQuery("SELECT id, marker FROM test_native_ctas_unknown", "VALUES (1, NULL)");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS test_native_ctas_unknown");
        }
    }

    @Test
    public void testCtasAllUnknownColumnsRejected()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_native_ctas_all_unknown");
        assertQueryFails(
                "CREATE TABLE test_native_ctas_all_unknown WITH (\"format-version\" = '3') AS SELECT NULL AS marker",
                ".*Writing to a table where every column is of type UNKNOWN is not supported.*");
        assertQuerySucceeds("DROP TABLE IF EXISTS test_native_ctas_all_unknown");
    }

    @Test
    public void testWriteTableOfOnlyUnknownColumnsRejected()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_native_only_unknown");
        try {
            assertQuerySucceeds("CREATE TABLE test_native_only_unknown (unknown_column UNKNOWN) WITH (\"format-version\" = '3')");
            assertQueryFails(
                    "INSERT INTO test_native_only_unknown VALUES NULL",
                    ".*Writing to a table where every column is of type UNKNOWN is not supported.*");
            assertQuery("SELECT count(*) FROM test_native_only_unknown", "VALUES 0");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS test_native_only_unknown");
        }
    }

    @Test
    public void testDropLastNonUnknownColumnRejectsWrite()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_native_drop_non_unknown");
        try {
            assertQuerySucceeds("CREATE TABLE test_native_drop_non_unknown (id INTEGER, unknown_column UNKNOWN) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO test_native_drop_non_unknown VALUES (1, NULL)");
            assertQuerySucceeds("ALTER TABLE test_native_drop_non_unknown DROP COLUMN id");
            assertQueryFails(
                    "INSERT INTO test_native_drop_non_unknown VALUES NULL",
                    ".*Writing to a table where every column is of type UNKNOWN is not supported.*");
            assertQuery("SELECT count(*) FROM test_native_drop_non_unknown", "VALUES 1");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS test_native_drop_non_unknown");
        }
    }

    @Test
    public void testAddColumnToAllUnknownTableRestoresWritability()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_native_add_col_restores");
        try {
            assertQuerySucceeds("CREATE TABLE test_native_add_col_restores (marker UNKNOWN) WITH (\"format-version\" = '3')");
            assertQueryFails(
                    "INSERT INTO test_native_add_col_restores VALUES NULL",
                    ".*Writing to a table where every column is of type UNKNOWN is not supported.*");
            assertQuerySucceeds("ALTER TABLE test_native_add_col_restores ADD COLUMN id INTEGER");
            assertQuerySucceeds("INSERT INTO test_native_add_col_restores (id, marker) VALUES (1, NULL)");
            assertQuery("SELECT id, marker FROM test_native_add_col_restores", "VALUES (1, NULL)");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS test_native_add_col_restores");
        }
    }

    @Test
    public void testRenameAndDropUnknownColumn()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS test_native_rename_unknown");
        try {
            assertQuerySucceeds("CREATE TABLE test_native_rename_unknown (id INTEGER) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO test_native_rename_unknown VALUES 1");
            assertQuerySucceeds("ALTER TABLE test_native_rename_unknown ADD COLUMN unknown_column UNKNOWN");
            assertQuerySucceeds("ALTER TABLE test_native_rename_unknown RENAME COLUMN unknown_column TO renamed_unknown");
            assertQuery("SELECT * FROM test_native_rename_unknown", "VALUES (1, NULL)");
            assertQuerySucceeds("ALTER TABLE test_native_rename_unknown DROP COLUMN renamed_unknown");
            assertQuery("SELECT * FROM test_native_rename_unknown", "VALUES 1");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS test_native_rename_unknown");
        }
    }

    @Test
    public void testUnknownColumnMetadataTablesAndStatistics()
    {
        assertQuerySucceeds("SELECT * FROM \"test_native_unknown_metadata$partitions\"");
        assertQuerySucceeds("SELECT * FROM \"test_native_unknown_metadata$files\"");
        assertQuerySucceeds("SELECT * FROM \"test_native_unknown_metadata$manifests\"");
        assertQuerySucceeds("SHOW STATS FOR test_native_unknown_metadata");
        assertQuerySucceeds("ANALYZE test_native_unknown_metadata");
        assertQuerySucceeds("SHOW COLUMNS FROM test_native_unknown_metadata");
    }

    @Test
    public void testInformationSchemaColumns()
    {
        assertQuery(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'test_native_unknown_read' ORDER BY column_name",
                "VALUES ('id', 'integer'), ('name', 'varchar'), ('unknown_column', 'unknown')");
    }
}
