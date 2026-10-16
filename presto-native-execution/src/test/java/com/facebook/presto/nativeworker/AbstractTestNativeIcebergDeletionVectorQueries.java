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
import static org.testng.Assert.assertEquals;

/**
 * End-to-end tests for Iceberg format-version 3 row-level DELETE / UPDATE / MERGE,
 * which land as Puffin deletion vectors written by the native (Prestissimo) worker.
 *
 * <p>Unlike the V2 position-delete suite, every mutating statement must run on the
 * native query runner: only the Velox IcebergDeletionVectorSink can write a Puffin
 * blob, so a Java-only coordinator cannot complete these statements. The Java
 * expected-runner is still used as the shared metastore so that reads can
 * cross-check against it where useful.
 */
public abstract class AbstractTestNativeIcebergDeletionVectorQueries
        extends AbstractTestQueryFramework
{
    // Storage format for the shared `nation` seed table. The data file format
    // is independent of the V3 deletion-vector mechanism (DVs are always Puffin
    // blobs regardless of data file format), so we pick PARQUET to match the
    // V2 position-delete suite -- any native reader has to handle it.
    private static final String SEED_STORAGE_FORMAT = "PARQUET";

    @Override
    protected void createTables()
    {
        // Seed via the Java expected runner. The native and expected runners
        // share a data directory, so the nation table created here is visible
        // to native queries. We go through the Java runner because the native
        // worker isn't registered as a TPCH-capable node in this harness --
        // SELECT ... FROM tpch.tiny.nation on native hits "No nodes available".
        QueryRunner expected = (QueryRunner) getExpectedQueryRunner();
        createNationWithFormat(expected, SEED_STORAGE_FORMAT);
    }

    @Test
    public void testV3DeleteOnNonPartitionedTable()
    {
        QueryRunner nativeRunner = getQueryRunner();
        String table = "iceberg_native_v3_dv_test";
        try {
            nativeRunner.execute(
                    "CREATE TABLE " + table + " (nationkey BIGINT, name VARCHAR, regionkey BIGINT, comment VARCHAR)" +
                            " WITH (\"format-version\" = '3')");
            nativeRunner.execute("INSERT INTO " + table + " SELECT nationkey, name, regionkey, comment FROM nation");

            assertRowCount("SELECT * FROM " + table, 25);
            assertRowCount("SELECT * FROM " + table + " WHERE nationkey = 10", 1);

            // DELETE one row -- commits a Puffin deletion vector for the data file.
            nativeRunner.execute("DELETE FROM " + table + " WHERE nationkey = 10");
            assertRowCount("SELECT * FROM " + table, 24);
            assertRowCount("SELECT * FROM " + table + " WHERE nationkey = 10", 0);

            // DELETE another row from the same data file -- must REPLACE the prior DV,
            // not add a second one. One-DV-per-data-file is a V3 invariant; a second
            // DV on the same file would be visible as a corrupt table state on later
            // reads or in the manifest list.
            nativeRunner.execute("DELETE FROM " + table + " WHERE nationkey = 20");
            assertRowCount("SELECT * FROM " + table, 23);
            assertRowCount("SELECT * FROM " + table + " WHERE nationkey = 20", 0);
            assertRowCount("SELECT * FROM " + table + " WHERE nationkey = 10", 0);

            // DELETE a row that does not exist -- no rows should change.
            nativeRunner.execute("DELETE FROM " + table + " WHERE nationkey = 100");
            assertRowCount("SELECT * FROM " + table, 23);

            // DELETE everything.
            nativeRunner.execute("DELETE FROM " + table + " WHERE nationkey >= 0");
            assertRowCount("SELECT * FROM " + table, 0);

            // Reinsert rows (some with NULLs) and delete through NULL predicates.
            nativeRunner.execute("INSERT INTO " + table + " VALUES" +
                    " (NULL, 'INDIA', 1, 'NULL Value Row')," +
                    " (NULL, 'USA', 2, 'NULL Value Row')," +
                    " (NULL, 'UK', 3, 'NULL Value Row')," +
                    " (25, 'AUSTRALIA', 1, 'Non-NULL Value Row')");
            assertRowCount("SELECT * FROM " + table, 4);

            nativeRunner.execute("DELETE FROM " + table + " WHERE nationkey = 25");
            assertRowCount("SELECT * FROM " + table, 3);

            nativeRunner.execute("DELETE FROM " + table + " WHERE nationkey IS NULL");
            assertRowCount("SELECT * FROM " + table, 0);
        }
        finally {
            nativeRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testV3DeleteOnPartitionedTable()
    {
        // Regression test for the partition-spec bug the PR fixes. The prior
        // toIcebergDeletionVector reconstructed the replaced DV with
        // PartitionSpec.unpartitioned(), so ManifestFilterManager pruned away the
        // real (partitioned) manifest containing the old DV and silently left two
        // DVs on the same data file. The failure only surfaces on partitioned V3
        // tables under repeated mutation of the same file.
        QueryRunner nativeRunner = getQueryRunner();
        String table = "iceberg_native_v3_dv_partitioned_test";
        try {
            nativeRunner.execute(
                    "CREATE TABLE " + table + " (nationkey BIGINT, name VARCHAR, comment VARCHAR, regionkey VARCHAR)" +
                            " WITH (\"format-version\" = '3', partitioning = ARRAY['regionkey'])");
            nativeRunner.execute("INSERT INTO " + table +
                    " SELECT nationkey, name, comment, CAST(regionkey AS VARCHAR) FROM nation");

            assertRowCount("SELECT * FROM " + table, 25);
            assertRowCount("SELECT * FROM " + table + " WHERE regionkey = '0'", 5);

            // DELETE on a non-partition column -- data file in one partition is
            // the one that gets a DV.
            nativeRunner.execute("DELETE FROM " + table + " WHERE nationkey = 10");
            assertRowCount("SELECT * FROM " + table, 24);
            assertRowCount("SELECT * FROM " + table + " WHERE nationkey = 10", 0);

            // DELETE another non-partition-col row within the SAME partition. This
            // exercises the DV replacement path: the second DV for that data file
            // must replace the first one, not sit alongside it.
            nativeRunner.execute("DELETE FROM " + table + " WHERE nationkey = 11");
            assertRowCount("SELECT * FROM " + table, 23);
            assertRowCount("SELECT * FROM " + table + " WHERE nationkey IN (10, 11)", 0);

            // DELETE on a partition column -- drops the whole partition.
            nativeRunner.execute("DELETE FROM " + table + " WHERE regionkey = '0'");
            assertRowCount("SELECT * FROM " + table, 18);
            assertRowCount("SELECT * FROM " + table + " WHERE regionkey = '0'", 0);

            // Insert new rows including NULL partition values, then DELETE where
            // the partition column is NULL.
            nativeRunner.execute("INSERT INTO " + table + " VALUES" +
                    " (25, 'INDIA', 'NULL Value Row', NULL)," +
                    " (26, 'USA', 'NULL Value Row', NULL)," +
                    " (27, 'UK', 'NULL Value Row', NULL)," +
                    " (28, 'AUSTRALIA', 'Non-NULL Value Row', '5')");
            assertRowCount("SELECT * FROM " + table, 22);

            nativeRunner.execute("DELETE FROM " + table + " WHERE regionkey IS NULL");
            assertRowCount("SELECT * FROM " + table, 19);
            assertRowCount("SELECT * FROM " + table + " WHERE regionkey IS NULL", 0);
        }
        finally {
            nativeRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testV3UpdateCommitsDeletionVector()
    {
        // V3 UPDATE is planned as a MERGE on native: the old row position is
        // recorded in a Puffin DV and the new row is written as new data. If the
        // DV path were broken, the updated rows would either not disappear from
        // the base file (producing duplicates on read) or the UPDATE would fail
        // outright at the Velox sink.
        QueryRunner nativeRunner = getQueryRunner();
        String table = "iceberg_native_v3_update_test";
        try {
            nativeRunner.execute(
                    "CREATE TABLE " + table + " (id INTEGER, name VARCHAR, status VARCHAR, score DOUBLE)" +
                            " WITH (\"format-version\" = '3')");
            nativeRunner.execute("INSERT INTO " + table + " VALUES" +
                    " (1, 'alice', 'new', 10.0)," +
                    " (2, 'bob', 'new', 20.0)," +
                    " (3, 'carol', 'new', 30.0)");
            assertRowCount("SELECT * FROM " + table + " WHERE status = 'new'", 3);

            nativeRunner.execute("UPDATE " + table + " SET status = 'done' WHERE id = 2");

            assertRowCount("SELECT * FROM " + table, 3);
            assertRowCount("SELECT * FROM " + table + " WHERE status = 'done'", 1);
            assertRowCount("SELECT * FROM " + table + " WHERE status = 'new'", 2);
            assertRowCount("SELECT * FROM " + table + " WHERE id = 2 AND status = 'done' AND score = 20.0", 1);

            // Update the same row again -- exercises DV replacement for the
            // already-updated row's new data file.
            nativeRunner.execute("UPDATE " + table + " SET score = 99.0 WHERE id = 2");
            assertRowCount("SELECT * FROM " + table, 3);
            assertRowCount("SELECT * FROM " + table + " WHERE id = 2 AND score = 99.0", 1);
        }
        finally {
            nativeRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testV3MergeCommitsDeletionVector()
    {
        // MERGE exercises all three WHEN branches: WHEN MATCHED UPDATE / DELETE
        // and WHEN NOT MATCHED INSERT. The MATCHED branches both land as DVs.
        QueryRunner nativeRunner = getQueryRunner();
        String target = "iceberg_native_v3_merge_target";
        String source = "iceberg_native_v3_merge_source";
        try {
            nativeRunner.execute(
                    "CREATE TABLE " + target + " (id INTEGER, name VARCHAR, value DOUBLE)" +
                            " WITH (\"format-version\" = '3')");
            nativeRunner.execute("INSERT INTO " + target + " VALUES" +
                    " (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0), (4, 'd', 4.0)");

            nativeRunner.execute(
                    "CREATE TABLE " + source + " (id INTEGER, name VARCHAR, value DOUBLE)" +
                            " WITH (\"format-version\" = '3')");
            // id=2 -> update, id=3 -> delete (marker value < 0), id=5 -> insert.
            nativeRunner.execute("INSERT INTO " + source + " VALUES (2, 'B', 20.0), (3, 'c', -1.0), (5, 'e', 5.0)");

            nativeRunner.execute(
                    "MERGE INTO " + target + " t USING " + source + " s ON t.id = s.id" +
                            " WHEN MATCHED AND s.value < 0 THEN DELETE" +
                            " WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value" +
                            " WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)");

            assertRowCount("SELECT * FROM " + target, 4);
            assertRowCount("SELECT * FROM " + target + " WHERE id = 2 AND name = 'B' AND value = 20.0", 1);
            assertRowCount("SELECT * FROM " + target + " WHERE id = 3", 0);
            assertRowCount("SELECT * FROM " + target + " WHERE id = 5 AND name = 'e' AND value = 5.0", 1);
            assertRowCount("SELECT * FROM " + target + " WHERE id IN (1, 4)", 2);
        }
        finally {
            nativeRunner.execute("DROP TABLE IF EXISTS " + source);
            nativeRunner.execute("DROP TABLE IF EXISTS " + target);
        }
    }

    @Test
    public void testV3DeleteDoesNotRequireMergeOnReadProperty()
    {
        // V2 requires write.delete.mode = 'merge-on-read' to accept row-level
        // DELETE. V3 drops that requirement -- the DV write path is the only
        // option, so the property is implied. Confirm a V3 table created without
        // the property still accepts DELETE on native.
        QueryRunner nativeRunner = getQueryRunner();
        String table = "iceberg_native_v3_dv_default_props";
        try {
            nativeRunner.execute(
                    "CREATE TABLE " + table + " (id INTEGER, name VARCHAR)" +
                            " WITH (\"format-version\" = '3')");
            nativeRunner.execute("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            nativeRunner.execute("DELETE FROM " + table + " WHERE id = 2");
            assertRowCount("SELECT * FROM " + table, 2);
            assertRowCount("SELECT * FROM " + table + " WHERE id = 2", 0);
        }
        finally {
            nativeRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    private void assertRowCount(String sql, int expected)
    {
        assertEquals(getQueryRunner().execute(sql).getRowCount(), expected);
    }
}
