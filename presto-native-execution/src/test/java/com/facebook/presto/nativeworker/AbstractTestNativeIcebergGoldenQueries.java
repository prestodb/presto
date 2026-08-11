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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNationWithFormat;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * End-to-end "golden" coverage that a native (Prestissimo) worker reads and writes
 * Iceberg correctly across the operations OSS supports today.
 * <p>
 * The oracle is the repo idiom: {@link #assertQuery(String)} runs the query on both the
 * native worker and a Java reference Iceberg runner (see
 * {@link #createExpectedQueryRunner()}) and compares results. Both runners are wired to
 * the same storage, so table mutations are issued once through the Java reference runner
 * and the read is then compared across both engines.
 * <p>
 * Scope is intentionally capped at Iceberg format-version 2 (position/equality deletes);
 * there is no V3 deletion-vector write path exercised here (native V3 write execution
 * depends on the Velox deletion-vector sink, which is out of scope for these tests).
 */
public abstract class AbstractTestNativeIcebergGoldenQueries
        extends AbstractTestQueryFramework
{
    protected abstract String getStorageFormat();

    @Override
    protected void createTables()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();

        createNationWithFormat(javaQueryRunner, getStorageFormat());

        javaQueryRunner.execute("DROP TABLE IF EXISTS golden_region");
        javaQueryRunner.execute("CREATE TABLE golden_region AS SELECT * FROM tpch.tiny.region");

        // Typed table exercising decimal/timestamp/date/varbinary/real/double/boolean + NULLs.
        javaQueryRunner.execute("DROP TABLE IF EXISTS golden_types");
        javaQueryRunner.execute(
                "CREATE TABLE golden_types (" +
                        "id BIGINT, " +
                        "dec DECIMAL(10, 2), " +
                        "ts TIMESTAMP, " +
                        "dt DATE, " +
                        "vb VARBINARY, " +
                        "rl REAL, " +
                        "dbl DOUBLE, " +
                        "bl BOOLEAN)");
        javaQueryRunner.execute(
                "INSERT INTO golden_types VALUES " +
                        "(1, DECIMAL '12.34', TIMESTAMP '2022-04-09 10:11:12.000', DATE '2022-04-09', " +
                        "CAST('abc' AS VARBINARY), REAL '1.5', DOUBLE '2.5', true), " +
                        "(2, DECIMAL '-0.10', TIMESTAMP '1993-01-01 00:00:00.000', DATE '1993-01-01', " +
                        "CAST('' AS VARBINARY), REAL '0.0', DOUBLE '-3.25', false), " +
                        "(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");

        // Nested types: row / array / map.
        javaQueryRunner.execute("DROP TABLE IF EXISTS golden_nested");
        javaQueryRunner.execute(
                "CREATE TABLE golden_nested (" +
                        "id BIGINT, " +
                        "arr ARRAY(INTEGER), " +
                        "mp MAP(VARCHAR, INTEGER), " +
                        "rw ROW(a INTEGER, b VARCHAR))");
        javaQueryRunner.execute(
                "INSERT INTO golden_nested VALUES " +
                        "(1, ARRAY[1, 2, 3], MAP(ARRAY['x', 'y'], ARRAY[10, 20]), ROW(1, 'one')), " +
                        "(2, ARRAY[], MAP(ARRAY[], ARRAY[]), ROW(2, 'two')), " +
                        "(3, NULL, NULL, NULL)");
    }

    // ------------------------------------------------------------------
    // DDL / DML
    // ------------------------------------------------------------------

    @Test
    public void testCreateTableAsSelect()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        try {
            javaQueryRunner.execute("CREATE TABLE golden_ctas AS SELECT * FROM nation");
            assertQuery("SELECT * FROM golden_ctas");
            assertQuery("SELECT count(*) FROM golden_ctas", "VALUES 25");
        }
        finally {
            javaQueryRunner.execute("DROP TABLE IF EXISTS golden_ctas");
        }
    }

    @Test
    public void testCreateThenInsert()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        try {
            javaQueryRunner.execute("CREATE TABLE golden_insert (nationkey BIGINT, name VARCHAR)");
            // Empty-table read.
            assertQuery("SELECT * FROM golden_insert");
            assertQuery("SELECT count(*) FROM golden_insert", "VALUES 0");

            javaQueryRunner.execute("INSERT INTO golden_insert VALUES (1, 'ONE'), (2, 'TWO')");
            javaQueryRunner.execute("INSERT INTO golden_insert SELECT nationkey, name FROM nation WHERE nationkey < 3");

            assertQuery("SELECT * FROM golden_insert");
            assertQuery("SELECT nationkey FROM golden_insert ORDER BY nationkey");
        }
        finally {
            javaQueryRunner.execute("DROP TABLE IF EXISTS golden_insert");
        }
    }

    // ------------------------------------------------------------------
    // Partitioning transforms + partition pruning
    // ------------------------------------------------------------------

    @Test
    public void testIdentityPartitioning()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        try {
            javaQueryRunner.execute(
                    "CREATE TABLE golden_part_identity (nationkey BIGINT, name VARCHAR, regionkey BIGINT) " +
                            "WITH (partitioning = ARRAY['regionkey'])");
            javaQueryRunner.execute("INSERT INTO golden_part_identity SELECT nationkey, name, regionkey FROM nation");

            assertQuery("SELECT * FROM golden_part_identity");
            // Partition pruning filter.
            assertQuery("SELECT * FROM golden_part_identity WHERE regionkey = 1");
            assertQuery("SELECT count(*) FROM golden_part_identity WHERE regionkey = 1", "VALUES 5");
        }
        finally {
            javaQueryRunner.execute("DROP TABLE IF EXISTS golden_part_identity");
        }
    }

    @Test
    public void testBucketAndTruncatePartitioning()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        try {
            javaQueryRunner.execute(
                    "CREATE TABLE golden_part_bucket (nationkey BIGINT, name VARCHAR) " +
                            "WITH (partitioning = ARRAY['bucket(nationkey, 4)'])");
            javaQueryRunner.execute("INSERT INTO golden_part_bucket SELECT nationkey, name FROM nation");
            assertQuery("SELECT * FROM golden_part_bucket");
            assertQuery("SELECT * FROM golden_part_bucket WHERE nationkey = 10");

            javaQueryRunner.execute(
                    "CREATE TABLE golden_part_truncate (name VARCHAR, nationkey BIGINT) " +
                            "WITH (partitioning = ARRAY['truncate(name, 1)'])");
            javaQueryRunner.execute("INSERT INTO golden_part_truncate SELECT name, nationkey FROM nation");
            assertQuery("SELECT * FROM golden_part_truncate");
            assertQuery("SELECT * FROM golden_part_truncate WHERE name = 'INDIA'");
        }
        finally {
            javaQueryRunner.execute("DROP TABLE IF EXISTS golden_part_bucket");
            javaQueryRunner.execute("DROP TABLE IF EXISTS golden_part_truncate");
        }
    }

    @Test
    public void testTemporalPartitioning()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        try {
            // Iceberg rejects multiple time transforms on the same source column as redundant
            // (month already encodes year, day encodes month, hour encodes day), so each
            // granularity gets its own source column.
            javaQueryRunner.execute(
                    "CREATE TABLE golden_part_temporal (id BIGINT, ts TIMESTAMP, ts_y TIMESTAMP, ts_m TIMESTAMP, ts_h TIMESTAMP) " +
                            "WITH (partitioning = ARRAY['day(ts)', 'year(ts_y)', 'month(ts_m)', 'hour(ts_h)'])");
            javaQueryRunner.execute(
                    "INSERT INTO golden_part_temporal VALUES " +
                            "(1, TIMESTAMP '2022-04-09 10:11:12.000', TIMESTAMP '2022-04-09 10:11:12.000', TIMESTAMP '2022-04-09 10:11:12.000', TIMESTAMP '2022-04-09 10:11:12.000'), " +
                            "(2, TIMESTAMP '2022-03-18 01:02:03.000', TIMESTAMP '2022-03-18 01:02:03.000', TIMESTAMP '2022-03-18 01:02:03.000', TIMESTAMP '2022-03-18 01:02:03.000'), " +
                            "(3, TIMESTAMP '1993-01-01 23:59:59.000', TIMESTAMP '1993-01-01 23:59:59.000', TIMESTAMP '1993-01-01 23:59:59.000', TIMESTAMP '1993-01-01 23:59:59.000')");
            assertQuery("SELECT * FROM golden_part_temporal");
            assertQuery("SELECT * FROM golden_part_temporal WHERE ts >= TIMESTAMP '2000-01-01 00:00:00.000'");
        }
        finally {
            javaQueryRunner.execute("DROP TABLE IF EXISTS golden_part_temporal");
        }
    }

    // ------------------------------------------------------------------
    // Reads: projections, filters, aggregations, joins, ORDER BY / LIMIT
    // ------------------------------------------------------------------

    @Test
    public void testProjectionsAndFilters()
    {
        assertQuery("SELECT nationkey, name FROM nation WHERE regionkey = 1");
        assertQuery("SELECT name FROM nation WHERE nationkey IN (1, 5, 10)");
        assertQuery("SELECT * FROM nation WHERE name LIKE 'A%'");
    }

    @Test
    public void testAggregations()
    {
        assertQuery("SELECT regionkey, count(*), min(nationkey), max(nationkey) FROM nation GROUP BY regionkey");
        assertQuery("SELECT count(*) FROM nation", "VALUES 25");
    }

    @Test
    public void testJoinAcrossIcebergTables()
    {
        assertQuery(
                "SELECT n.name, r.name FROM nation n JOIN golden_region r ON n.regionkey = r.regionkey");
        assertQuery(
                "SELECT count(*) FROM nation n JOIN golden_region r ON n.regionkey = r.regionkey", "VALUES 25");
    }

    @Test
    public void testOrderByAndLimit()
    {
        assertQuery("SELECT nationkey FROM nation ORDER BY nationkey LIMIT 5");
        assertQuery("SELECT * FROM nation ORDER BY nationkey DESC LIMIT 3");
    }

    // ------------------------------------------------------------------
    // Types + NULL handling
    // ------------------------------------------------------------------

    @Test
    public void testTypesRoundTrip()
    {
        assertQuery("SELECT id FROM golden_types WHERE dec IS NULL", "VALUES 3");
        assertQuery("SELECT count(*) FROM golden_types WHERE bl", "VALUES 1");
    }

    @Test
    public void testNestedTypes()
    {
        assertQuery("SELECT * FROM golden_nested");
        assertQuery("SELECT id, arr[1], mp['x'], rw.b FROM golden_nested WHERE id = 1",
                "VALUES (1, 1, 10, 'one')");
        assertQuery("SELECT id FROM golden_nested WHERE arr IS NULL", "VALUES 3");
    }

    // ------------------------------------------------------------------
    // Hidden / metadata columns
    // ------------------------------------------------------------------

    @Test
    public void testHiddenColumns()
    {
        assertQuery("SELECT \"$path\", * FROM nation");
        assertQuery("SELECT \"$data_sequence_number\", * FROM nation");

        String filePath = (String) computeActual("SELECT \"$path\" FROM nation LIMIT 1").getOnlyValue();
        assertQuery(format("SELECT * FROM nation WHERE \"$path\" = '%s'", filePath));
        assertEquals(
                (Long) computeActual(format("SELECT count(*) FROM nation WHERE \"$path\" = '%s'", "non-existent-path"))
                        .getOnlyValue(),
                0L);
    }

    @Test
    public void testMetadataTables()
    {
        assertQuery("SELECT count(*) >= 1 FROM \"nation$snapshots\"", "VALUES true");
        assertQuery("SELECT count(*) >= 1 FROM \"nation$files\"", "VALUES true");
        assertQuery("SELECT count(*) >= 1 FROM \"nation$manifests\"", "VALUES true");
        assertQuery("SELECT count(*) >= 1 FROM \"nation$history\"", "VALUES true");
        // $partitions is compared value-for-value across engines.
        assertQuery("SELECT row_count FROM \"nation$partitions\"");
    }

    // ------------------------------------------------------------------
    // V2 position deletes (merge-on-read) — exercises the PR's native
    // delete-handle path + existingDeletionVectors read-tolerance.
    // ------------------------------------------------------------------

    @Test
    public void testPositionDeletesNonPartitioned()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        try {
            javaQueryRunner.execute("CREATE TABLE golden_delete AS SELECT * FROM nation");
            assertQuery("SELECT count(*) FROM golden_delete", "VALUES 25");

            javaQueryRunner.execute("DELETE FROM golden_delete WHERE nationkey = 10");
            assertQuery("SELECT count(*) FROM golden_delete", "VALUES 24");
            assertQuery("SELECT * FROM golden_delete WHERE nationkey = 10");

            // Second delete file.
            javaQueryRunner.execute("DELETE FROM golden_delete WHERE nationkey = 20");
            assertQuery("SELECT count(*) FROM golden_delete", "VALUES 23");
            assertQuery("SELECT * FROM golden_delete");

            // Insert including NULLs, then delete across the mix.
            javaQueryRunner.execute("INSERT INTO golden_delete VALUES (100, 'NEW', 1, 'row')");
            javaQueryRunner.execute("DELETE FROM golden_delete WHERE nationkey = 100");
            assertQuery("SELECT count(*) FROM golden_delete", "VALUES 23");
        }
        finally {
            javaQueryRunner.execute("DROP TABLE IF EXISTS golden_delete");
        }
    }

    @Test
    public void testPositionDeletesPartitioned()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        try {
            javaQueryRunner.execute(
                    "CREATE TABLE golden_delete_part (nationkey BIGINT, name VARCHAR, regionkey BIGINT) " +
                            "WITH (partitioning = ARRAY['regionkey'])");
            javaQueryRunner.execute("INSERT INTO golden_delete_part SELECT nationkey, name, regionkey FROM nation");
            assertQuery("SELECT count(*) FROM golden_delete_part", "VALUES 25");

            // Delete on non-partition column.
            javaQueryRunner.execute("DELETE FROM golden_delete_part WHERE nationkey = 10");
            assertQuery("SELECT count(*) FROM golden_delete_part", "VALUES 24");

            // Delete on partition column (whole-partition delete).
            javaQueryRunner.execute("DELETE FROM golden_delete_part WHERE regionkey = 0");
            assertQuery("SELECT count(*) FROM golden_delete_part", "VALUES 19");
            assertQuery("SELECT * FROM golden_delete_part");
        }
        finally {
            javaQueryRunner.execute("DROP TABLE IF EXISTS golden_delete_part");
        }
    }

    // ------------------------------------------------------------------
    // Schema evolution
    // ------------------------------------------------------------------

    @Test
    public void testSchemaEvolution()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        try {
            javaQueryRunner.execute("CREATE TABLE golden_evolve (a BIGINT, b VARCHAR)");
            javaQueryRunner.execute("INSERT INTO golden_evolve VALUES (1, 'one'), (2, 'two')");

            // ADD COLUMN — old rows read back NULL for the new column.
            javaQueryRunner.execute("ALTER TABLE golden_evolve ADD COLUMN c DOUBLE");
            javaQueryRunner.execute("INSERT INTO golden_evolve VALUES (3, 'three', 3.5)");
            assertQuery("SELECT * FROM golden_evolve");
            assertQuery("SELECT a, c FROM golden_evolve WHERE a = 1", "VALUES (1, CAST(NULL AS DOUBLE))");

            // RENAME COLUMN.
            javaQueryRunner.execute("ALTER TABLE golden_evolve RENAME COLUMN b TO b_renamed");
            assertQuery("SELECT a, b_renamed FROM golden_evolve");

            // DROP COLUMN.
            javaQueryRunner.execute("ALTER TABLE golden_evolve DROP COLUMN c");
            assertQuery("SELECT * FROM golden_evolve");
        }
        finally {
            javaQueryRunner.execute("DROP TABLE IF EXISTS golden_evolve");
        }
    }

    // ------------------------------------------------------------------
    // Time travel
    // ------------------------------------------------------------------

    @Test
    public void testTimeTravel()
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        try {
            javaQueryRunner.execute("CREATE TABLE golden_time_travel (id BIGINT)");
            javaQueryRunner.execute("INSERT INTO golden_time_travel VALUES 1");
            long firstSnapshot = getLatestSnapshotId("golden_time_travel");
            String firstTimestamp = getLatestTimestamp("golden_time_travel");

            javaQueryRunner.execute("INSERT INTO golden_time_travel VALUES 2");

            assertQuery(
                    format("SELECT * FROM golden_time_travel FOR VERSION AS OF %d", firstSnapshot),
                    "VALUES 1");
            assertQuery(
                    format("SELECT * FROM golden_time_travel FOR TIMESTAMP AS OF TIMESTAMP '%s'", firstTimestamp),
                    "VALUES 1");
            assertQuery("SELECT * FROM golden_time_travel", "VALUES (1), (2)");
        }
        finally {
            javaQueryRunner.execute("DROP TABLE IF EXISTS golden_time_travel");
        }
    }

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeActual(
                format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1", tableName))
                .getOnlyValue();
    }

    private String getLatestTimestamp(String tableName)
    {
        return (String) computeActual(
                format("SELECT cast(made_current_at AS VARCHAR) FROM \"%s$history\" ORDER BY made_current_at DESC LIMIT 1", tableName))
                .getOnlyValue();
    }
}
