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
package com.facebook.presto.nativetests.iceberg;

import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFileFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;
import java.util.regex.Pattern;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Row-level DELETE, UPDATE and MERGE on Iceberg V3 tables, executed on native workers, which write
 * Puffin deletion vectors (DVs). Every test checks the rows against literal values or an in-memory
 * model, and checks the V3 delete invariants through the Iceberg API: position deletes are Puffin
 * DVs, every data file has at most one DV, and a later change to a data file replaces its DV
 * rather than adding a second one.
 */
public class TestIcebergV3DeletionVectorDml
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "tpch";
    private static final long SEED = 20261027L;
    private static final String NATIVE_ONLY = "Iceberg V3 .*requires? the native \\(Prestissimo\\) worker";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testDeleteWritesOneDeletionVectorPerDataFile()
    {
        String table = "v3_dv_dml_delete_files";
        createTable(table, "id bigint, v varchar");
        for (int batch = 0; batch < 4; batch++) {
            assertUpdate(format("INSERT INTO %s SELECT x + %s, 'b%s' FROM (%s)", table, batch * 1000, batch, range(1000)), 1000);
        }
        List<String> dataFilesBefore = dataFilePaths(table);

        assertUpdate("DELETE FROM " + table + " WHERE id % 7 = 0", 571);
        assertEquals(dataFilePaths(table), dataFilesBefore, "a DELETE must not rewrite data files");
        assertEquals(deletionVectorCount(table), (long) dataFilesBefore.size(), "every data file had a deleted row");
        assertEquals(deletedRowsInDeletionVectors(table), 571L);
        assertDeletionVectorInvariants(table);
        assertQuery("SELECT count(*), sum(id) FROM " + table, "SELECT count(*), sum(x) FROM (" + range(4000) + ") WHERE x % 7 <> 0");
        dropTable(table);
    }

    @Test
    public void testDeleteOnlyTouchesMatchingDataFiles()
    {
        String table = "v3_dv_dml_delete_one_file";
        createTable(table, "id bigint, batch integer");
        Set<String> batchOneFiles = new HashSet<>();
        for (int batch = 0; batch < 3; batch++) {
            Set<String> before = new HashSet<>(dataFilePaths(table));
            assertUpdate(format("INSERT INTO %s SELECT x + %s, %s FROM (%s)", table, batch * 100, batch, range(100)), 100);
            if (batch == 1) {
                batchOneFiles.addAll(dataFilePaths(table));
                batchOneFiles.removeAll(before);
            }
        }
        assertUpdate("DELETE FROM " + table + " WHERE batch = 1 AND id % 2 = 0", 50);
        assertTrue(deletionVectorCount(table) >= 1);
        assertEquals(deletionVectorCountWhere(table, batchOneFiles::contains), deletionVectorCount(table), "only batch-1 data files have deleted rows");
        assertQuery("SELECT batch, count(*) FROM " + table + " GROUP BY batch", "VALUES (0, 100), (1, 50), (2, 100)");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testRepeatedDeletesReplaceDeletionVector()
    {
        // Ten DELETEs against the same data files: each one must replace the previous DV of every
        // file it touches, so the deleted-row count in the live DVs grows while the DV count does not.
        String table = "v3_dv_dml_repeated_delete";
        createTable(table, "id bigint");
        assertUpdate(format("INSERT INTO %s %s", table, range(10_000)), 10_000);
        long dataFiles = dataFilePaths(table).size();
        long deleted = 0;
        for (int round = 1; round <= 10; round++) {
            long expected = scalarLong(format("SELECT count(*) FROM %s WHERE id %% 50 = %s", table, round));
            assertUpdate(format("DELETE FROM %s WHERE id %% 50 = %s", table, round), expected);
            deleted += expected;
            assertEquals(deletedRowsInDeletionVectors(table), deleted, "round " + round);
            assertTrue(deletionVectorCount(table) <= dataFiles, "round " + round + ": more DVs than data files");
            assertDeletionVectorInvariants(table);
        }
        assertEquals(scalarLong("SELECT count(*) FROM " + table), 10_000 - deleted);
        dropTable(table);
    }

    @Test
    public void testDeleteSameRowsTwice()
    {
        String table = "v3_dv_dml_delete_twice";
        createTable(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2), (3), (4)", 4);
        assertUpdate("DELETE FROM " + table + " WHERE id IN (1, 2)", 2);
        assertUpdate("DELETE FROM " + table + " WHERE id IN (1, 2)", 0);
        assertUpdate("DELETE FROM " + table + " WHERE id IN (2, 3)", 1);
        assertQuery("SELECT id FROM " + table, "VALUES 4");
        assertEquals(deletedRowsInDeletionVectors(table), 3L);
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testDeleteEveryRowThenInsert()
    {
        String table = "v3_dv_dml_delete_all";
        createTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 'c')", 1);
        assertUpdate("DELETE FROM " + table + " WHERE id > 0", 3);
        assertQuery("SELECT count(*) FROM " + table, "VALUES 0");
        assertDeletionVectorInvariants(table);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 'd')", 1);
        assertUpdate("DELETE FROM " + table + " WHERE v = 'd'", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (5, 'e')", 1);
        assertQuery("SELECT id, v FROM " + table, "VALUES (5, 'e')");
        dropTable(table);
    }

    @Test
    public void testUpdateWritesDeletionVectorAndNewRows()
    {
        String table = "v3_dv_dml_update";
        createTable(table, "id bigint, v varchar, n integer");
        assertUpdate(format("INSERT INTO %s SELECT x, 'old', 0 FROM (%s)", table, range(1000)), 1000);
        List<String> dataFilesBefore = dataFilePaths(table);

        assertRowLevelUpdate("UPDATE " + table + " SET v = 'new', n = n + 1 WHERE id % 10 = 3", 100, 100);
        assertTrue(dataFilePaths(table).containsAll(dataFilesBefore), "an UPDATE must keep the original data files");
        assertTrue(dataFilePaths(table).size() > dataFilesBefore.size(), "an UPDATE must write the new row versions");
        assertEquals(deletedRowsInDeletionVectors(table), 100L);
        assertQuery("SELECT v, n, count(*) FROM " + table + " GROUP BY v, n", "VALUES ('old', 0, 900), ('new', 1, 100)");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testUpdateSameRowRepeatedly()
    {
        // Each UPDATE deletes the previous version of the row, which lives in the file the previous UPDATE wrote.
        String table = "v3_dv_dml_update_same_row";
        createTable(table, "id integer, n integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 0), (2, 0), (3, 0)", 3);
        for (int round = 1; round <= 8; round++) {
            assertRowLevelUpdate("UPDATE " + table + " SET n = n + 1 WHERE id = 2", 1, 1);
            assertQuery("SELECT id, n FROM " + table, format("VALUES (1, 0), (2, %s), (3, 0)", round));
            assertDeletionVectorInvariants(table);
        }
        assertEquals(deletedRowsInDeletionVectors(table), 8L);
        dropTable(table);
    }

    @Test
    public void testUpdateEveryRowThenDeleteEveryRow()
    {
        String table = "v3_dv_dml_update_all";
        createTable(table, "id bigint, v bigint");
        assertUpdate(format("INSERT INTO %s SELECT x, x FROM (%s)", table, range(2000)), 2000);
        assertRowLevelUpdate("UPDATE " + table + " SET v = v * 2", 2000, 2000);
        assertQuery("SELECT count(*), sum(v) FROM " + table, "VALUES (2000, 4002000)");
        assertRowLevelUpdate("UPDATE " + table + " SET v = -v WHERE v > 2000", 1000, 1000);
        assertQuery("SELECT count(*), sum(v) FROM " + table, "VALUES (2000, -2000000)");
        assertDeletionVectorInvariants(table);
        assertUpdate("DELETE FROM " + table + " WHERE id > 0", 2000);
        assertQuery("SELECT count(*) FROM " + table, "VALUES 0");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testUpdateToNullAndFromNull()
    {
        String table = "v3_dv_dml_update_nulls";
        createTable(table, "id integer, v varchar, d double");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a', 1.5), (2, NULL, NULL), (3, 'c', 3.5)", 3);
        assertRowLevelUpdate("UPDATE " + table + " SET v = NULL, d = NULL WHERE id = 1", 1, 1);
        assertRowLevelUpdate("UPDATE " + table + " SET v = 'b', d = 2.5 WHERE v IS NULL AND id = 2", 1, 1);
        assertQuery("SELECT id, v, d FROM " + table, "VALUES (1, NULL, NULL), (2, 'b', 2.5), (3, 'c', 3.5)");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testMergeAllClauses()
    {
        String table = "v3_dv_dml_merge";
        createTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", 4);
        assertRowLevelUpdate("MERGE INTO " + table + " t USING (VALUES (1, 'D'), (2, 'U'), (5, 'I'), (6, 'I')) s(id, op) ON t.id = s.id "
                + "WHEN MATCHED AND s.op = 'D' THEN DELETE "
                + "WHEN MATCHED THEN UPDATE SET v = upper(t.v) "
                + "WHEN NOT MATCHED THEN INSERT VALUES (s.id, 'new')", 4, 1);
        assertQuery("SELECT id, v FROM " + table, "VALUES (2, 'B'), (3, 'c'), (4, 'd'), (5, 'new'), (6, 'new')");
        assertEquals(deletedRowsInDeletionVectors(table), 2L, "the deleted row and the old version of the updated row");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testMergeDeleteOnlyAndInsertOnly()
    {
        String table = "v3_dv_dml_merge_single_clause";
        createTable(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2), (3)", 3);
        assertUpdate("MERGE INTO " + table + " t USING (VALUES 2, 3, 9) s(id) ON t.id = s.id WHEN MATCHED THEN DELETE", 2);
        assertEquals(deletionVectorCount(table), 1L);
        long deletionVectorsBefore = deletedRowsInDeletionVectors(table);
        assertUpdate("MERGE INTO " + table + " t USING (VALUES 1, 4, 5) s(id) ON t.id = s.id WHEN NOT MATCHED THEN INSERT VALUES (s.id)", 2);
        assertEquals(deletedRowsInDeletionVectors(table), deletionVectorsBefore, "an insert-only MERGE must not write deletion vectors");
        assertQuery("SELECT id FROM " + table, "VALUES 1, 4, 5");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testMergeWithSourceFromAnotherTable()
    {
        String target = "v3_dv_dml_merge_target";
        String source = "v3_dv_dml_merge_source";
        createTable(target, "id bigint, v bigint");
        createTable(source, "id bigint, delta bigint, op varchar");
        assertUpdate(format("INSERT INTO %s SELECT x, x FROM (%s)", target, range(1000)), 1000);
        assertUpdate(format("INSERT INTO %s SELECT x, 100, CASE WHEN x %% 3 = 0 THEN 'D' ELSE 'U' END FROM (%s) WHERE x %% 2 = 0 OR x > 1000", source,
                range(1100)), 600);
        assertRowLevelUpdate(format("MERGE INTO %s t USING %s s ON t.id = s.id "
                + "WHEN MATCHED AND s.op = 'D' THEN DELETE "
                + "WHEN MATCHED THEN UPDATE SET v = t.v + s.delta "
                + "WHEN NOT MATCHED THEN INSERT VALUES (s.id, -s.id)", target, source), 600, 334);
        assertQuery("SELECT count(*), count_if(v < 0), count_if(v > id) FROM " + target, "VALUES (934, 100, 334)");
        assertDeletionVectorInvariants(target);
        dropTable(target);
        dropTable(source);
    }

    @Test
    public void testModelCheckedDmlRounds()
    {
        // Random DELETE, UPDATE and MERGE rounds over the same data files, each checked against an in-memory model.
        String table = "v3_dv_dml_rounds";
        int rows = 5000;
        createTable(table, "id bigint, v bigint");
        assertUpdate(format("INSERT INTO %s SELECT x, 0 FROM (%s)", table, range(rows)), rows);
        Map<Long, Long> model = new TreeMap<>();
        for (long id = 1; id <= rows; id++) {
            model.put(id, 0L);
        }
        Random random = new Random(SEED);
        long nextId = rows + 1;
        for (int round = 1; round <= 10; round++) {
            int deleteModulus = 11 + random.nextInt(30);
            int deleteRemainder = random.nextInt(deleteModulus);
            applyDelete(table, model, format("id %% %s = %s", deleteModulus, deleteRemainder), id -> id % deleteModulus == deleteRemainder);

            int updateModulus = 3 + random.nextInt(8);
            int updateRemainder = random.nextInt(updateModulus);
            long increment = round;
            applyUpdate(table, model, format("v + %s", increment), format("id %% %s = %s", updateModulus, updateRemainder),
                    id -> id % updateModulus == updateRemainder, value -> value + increment);

            long low = 1 + random.nextInt(rows - 200);
            long high = low + random.nextInt(150);
            long insertFrom = nextId;
            long insertTo = nextId + 20;
            nextId = insertTo + 1;
            applyMerge(table, model, low, high, insertFrom, insertTo, round);
            assertMatchesModel(table, model, "round " + round);
            assertDeletionVectorInvariants(table);
        }
        dropTable(table);
    }

    @Test
    public void testDmlOnPartitionedTable()
    {
        String table = "v3_dv_dml_partitioned";
        createTable(table, "id bigint, region varchar", "partitioning = ARRAY['region']");
        assertUpdate(format("INSERT INTO %s SELECT x, element_at(ARRAY['us', 'eu', 'apac'], CAST(x %% 3 AS integer) + 1) FROM (%s)", table, range(3000)), 3000);

        assertUpdate("DELETE FROM " + table + " WHERE region = 'eu' AND id % 10 = 0", 100);
        assertEquals(deletionVectorCountWhere(table, file -> file.contains("region=eu")), deletionVectorCount(table), "only eu files have deletes");
        assertRowLevelUpdate("UPDATE " + table + " SET region = 'us' WHERE region = 'apac' AND id <= 300", 100, 100);
        assertQuery("SELECT region, count(*) FROM " + table + " GROUP BY region", "VALUES ('us', 1100), ('eu', 900), ('apac', 900)");
        assertQuery("SELECT count(*) FROM \"" + table + "$partitions\"", "VALUES 3");
        assertDeletionVectorInvariants(table);

        assertUpdate("DELETE FROM " + table + " WHERE region = 'apac'", 900);
        assertQuery("SELECT region, count(*) FROM " + table + " GROUP BY region", "VALUES ('us', 1100), ('eu', 900)");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testDmlOnBucketedTable()
    {
        String table = "v3_dv_dml_bucketed";
        createTable(table, "id bigint, v varchar", "partitioning = ARRAY['bucket(id, 4)']");
        assertUpdate(format("INSERT INTO %s SELECT x, 'v' FROM (%s)", table, range(2000)), 2000);
        assertUpdate("DELETE FROM " + table + " WHERE id BETWEEN 100 AND 199", 100);
        assertRowLevelUpdate("UPDATE " + table + " SET v = 'u' WHERE id % 100 = 1", 19, 19);
        assertRowLevelUpdate("MERGE INTO " + table + " t USING (VALUES 1, 2, 5000) s(id) ON t.id = s.id "
                + "WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT VALUES (s.id, 'n')", 3, 0);
        assertQuery("SELECT count(*), count_if(v = 'u'), count_if(v = 'n') FROM " + table, "VALUES (1899, 18, 1)");
        // Rows written by UPDATE and MERGE carry partition data, so they land in the four existing buckets.
        assertQuery("SELECT count(*) FROM \"" + table + "$partitions\"", "VALUES 4");
        assertQuery("SELECT count(*) FROM " + table + " WHERE id = 5000", "VALUES 1");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testDmlAfterPartitionEvolution()
    {
        String table = "v3_dv_dml_partition_evolution";
        createTable(table, "id bigint, region varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'us'), (2, 'eu'), (3, 'us')", 3);
        loadTable(table).updateSpec().addField("region").commit();
        assertUpdate("INSERT INTO " + table + " VALUES (4, 'us'), (5, 'eu'), (6, 'apac')", 3);

        // One statement spanning files written under both partition specs.
        assertUpdate("DELETE FROM " + table + " WHERE region = 'us'", 3);
        assertRowLevelUpdate("UPDATE " + table + " SET region = 'emea' WHERE region = 'eu'", 2, 2);
        assertQuery("SELECT id, region FROM " + table, "VALUES (2, 'emea'), (5, 'emea'), (6, 'apac')");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testDmlAfterSchemaEvolution()
    {
        String table = "v3_dv_dml_schema_evolution";
        createTable(table, "id integer, a varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'x'), (2, 'y'), (3, 'z')", 3);
        assertUpdate("ALTER TABLE " + table + " ADD COLUMN b bigint");
        assertUpdate("ALTER TABLE " + table + " RENAME COLUMN a TO a2");
        assertUpdate("INSERT INTO " + table + " VALUES (4, 'w', 40)", 1);

        assertRowLevelUpdate("UPDATE " + table + " SET b = id * 10 WHERE b IS NULL AND id <> 2", 2, 2);
        assertUpdate("DELETE FROM " + table + " WHERE a2 = 'y'", 1);
        assertQuery("SELECT id, a2, b FROM " + table, "VALUES (1, 'x', 10), (3, 'z', 30), (4, 'w', 40)");
        assertUpdate("ALTER TABLE " + table + " DROP COLUMN a2");
        assertUpdate("DELETE FROM " + table + " WHERE b = 30", 1);
        assertQuery("SELECT id, b FROM " + table, "VALUES (1, 10), (4, 40)");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testDmlWithSubqueries()
    {
        String table = "v3_dv_dml_subqueries";
        String keys = "v3_dv_dml_subquery_keys";
        createTable(table, "id bigint, v bigint");
        createTable(keys, "id bigint");
        assertUpdate(format("INSERT INTO %s SELECT x, x FROM (%s)", table, range(100)), 100);
        assertUpdate(format("INSERT INTO %s SELECT x * 3 FROM (%s)", keys, range(20)), 20);

        assertUpdate(format("DELETE FROM %s WHERE id IN (SELECT id FROM %s)", table, keys), 20);
        // Native UPDATE runs as a MERGE, whose planner cannot translate a scalar subquery in SET.
        assertQueryFails(format("UPDATE %s SET v = (SELECT max(id) FROM %s) WHERE id > 90", table, keys),
                "(?s).*not yet implemented: expression translator for .*SubqueryExpression.*");
        assertRowLevelUpdate(format("UPDATE %s SET v = 60 WHERE id > 90", table), 10, 10);
        assertUpdate(format("DELETE FROM %s WHERE v > (SELECT avg(v) FROM %s) AND id < 50", table, table), 0);
        assertUpdate(format("DELETE FROM %s WHERE EXISTS (SELECT 1 FROM %s k WHERE k.id = %s.id + 1)", table, keys, table), 20);
        assertQuery("SELECT count(*), sum(CASE WHEN id > 90 THEN v ELSE 0 END) FROM " + table, "VALUES (60, 600)");
        assertDeletionVectorInvariants(table);
        dropTable(table);
        dropTable(keys);
    }

    @Test
    public void testDeleteThroughJoinDistributions()
    {
        // Subquery DELETEs run as joins. With a partitioned join, a correlated EXISTS repartitions
        // target rows by the join key, so rows of one data file reach several writer tasks that each
        // write a deletion vector for it. Iceberg merges those into one deletion vector at commit.
        for (String distribution : ImmutableList.of("BROADCAST", "PARTITIONED")) {
            for (String form : ImmutableList.of("IN", "EXISTS")) {
                String table = format("v3_dv_dml_join_%s_%s", distribution, form).toLowerCase(java.util.Locale.ENGLISH);
                String keys = table + "_keys";
                createTable(table, "id bigint, v bigint");
                createTable(keys, "id bigint");
                assertUpdate(format("INSERT INTO %s SELECT x, x FROM (%s)", table, range(10_000)), 10_000);
                assertUpdate(format("INSERT INTO %s SELECT x * 7 FROM (%s)", keys, range(1000)), 1000);
                String delete = form.equals("IN")
                        ? format("DELETE FROM %s WHERE id IN (SELECT id FROM %s)", table, keys)
                        : format("DELETE FROM %s WHERE EXISTS (SELECT 1 FROM %s k WHERE k.id = %s.id)", table, keys, table);
                com.facebook.presto.Session session = com.facebook.presto.Session.builder(getSession())
                        .setSystemProperty("join_distribution_type", distribution)
                        .build();
                long dataFiles = dataFilePaths(table).size();
                assertUpdate(session, delete, 1000);
                assertQuery("SELECT count(*), sum(id) FROM " + table, "SELECT count(*), sum(x) FROM (" + range(10_000) + ") WHERE x % 7 <> 0 OR x > 7000");
                assertEquals(deletedRowsInDeletionVectors(table), 1000L);
                assertTrue(deletionVectorCount(table) <= dataFiles, distribution + "/" + form + ": more deletion vectors than data files");
                assertDeletionVectorInvariants(table);

                // A second DELETE through the same plan replaces the merged deletion vector.
                assertUpdate(session, form.equals("IN")
                        ? format("DELETE FROM %s WHERE id + 1 IN (SELECT id FROM %s)", table, keys)
                        : format("DELETE FROM %s WHERE EXISTS (SELECT 1 FROM %s k WHERE k.id = %s.id + 1)", table, keys, table), 1000);
                assertEquals(deletedRowsInDeletionVectors(table), 2000L);
                assertQuery("SELECT count(*) FROM " + table, "VALUES 8000");
                assertDeletionVectorInvariants(table);
                dropTable(table);
                dropTable(keys);
            }
        }
    }

    @Test
    public void testDmlOnComplexTypes()
    {
        String table = "v3_dv_dml_complex";
        createTable(table, "id integer, tags array(varchar), attrs map(varchar, integer), info row(code integer, name varchar)");
        assertUpdate("INSERT INTO " + table + " VALUES "
                + "(1, ARRAY['a', 'b'], MAP(ARRAY['k'], ARRAY[1]), ROW(10, 'x')), "
                + "(2, ARRAY['c'], MAP(ARRAY['k'], ARRAY[2]), ROW(20, 'y')), "
                + "(3, ARRAY[], MAP(), NULL)", 3);
        assertUpdate("DELETE FROM " + table + " WHERE contains(tags, 'c')", 1);
        assertRowLevelUpdate("UPDATE " + table + " SET tags = concat(tags, 'z'), attrs = MAP(ARRAY['k'], ARRAY[9]), info = ROW(30, 'w') WHERE id = 3", 1, 1);
        assertQuery("SELECT id, cardinality(tags), attrs['k'], info.code FROM " + table, "VALUES (1, 2, 1, 10), (3, 1, 9, 30)");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testLargeScatteredAndContiguousDeletes()
    {
        String table = "v3_dv_dml_large";
        createTable(table, "id bigint, payload varchar");
        assertUpdate(format("INSERT INTO %s SELECT x, 'p' || CAST(x AS varchar) FROM (%s)", table, range(100_000)), 100_000);
        assertUpdate("DELETE FROM " + table + " WHERE id % 2 = 0", 50_000);
        assertUpdate("DELETE FROM " + table + " WHERE id BETWEEN 20001 AND 60000", 20_000);
        assertUpdate("DELETE FROM " + table + " WHERE id % 3 = 0", 10_000);
        long expected = 0;
        long expectedSum = 0;
        for (long id = 1; id <= 100_000; id++) {
            if (id % 2 == 1 && (id < 20001 || id > 60000) && id % 3 != 0) {
                expected++;
                expectedSum += id;
            }
        }
        assertQuery("SELECT count(*), sum(id) FROM " + table, format("VALUES (%s, %s)", expected, expectedSum));
        assertEquals(deletedRowsInDeletionVectors(table), 100_000 - expected);
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testTimeTravelAcrossDmlCommits()
    {
        String table = "v3_dv_dml_time_travel";
        createTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", 4);
        long afterInsert = currentSnapshotId(table);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        long afterDelete = currentSnapshotId(table);
        assertRowLevelUpdate("UPDATE " + table + " SET v = 'X' WHERE id = 2", 1, 1);
        long afterUpdate = currentSnapshotId(table);
        assertUpdate("DELETE FROM " + table + " WHERE id = 3", 1);

        assertQuery("SELECT id, v FROM " + table + " FOR VERSION AS OF " + afterInsert, "VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
        assertQuery("SELECT id, v FROM " + table + " FOR VERSION AS OF " + afterDelete, "VALUES (2, 'b'), (3, 'c'), (4, 'd')");
        assertQuery("SELECT id, v FROM " + table + " FOR VERSION AS OF " + afterUpdate, "VALUES (2, 'X'), (3, 'c'), (4, 'd')");
        assertQuery("SELECT id, v FROM " + table, "VALUES (2, 'X'), (4, 'd')");
        dropTable(table);
    }

    @Test
    public void testDmlAfterRollback()
    {
        // After a rollback, the next DELETE must replace the DV of the restored snapshot, not the discarded one.
        String table = "v3_dv_dml_rollback";
        createTable(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2), (3), (4), (5)", 5);
        assertUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        long afterFirstDelete = currentSnapshotId(table);
        assertUpdate("DELETE FROM " + table + " WHERE id = 2", 1);
        assertUpdate(format("CALL system.rollback_to_snapshot('%s', '%s', %s)", SCHEMA, table, afterFirstDelete));
        assertQuery("SELECT id FROM " + table, "VALUES 2, 3, 4, 5");
        assertUpdate("DELETE FROM " + table + " WHERE id = 5", 1);
        assertQuery("SELECT id FROM " + table, "VALUES 2, 3, 4");
        assertEquals(deletedRowsInDeletionVectors(table), 2L);
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testDmlBeforeAndAfterCompaction()
    {
        String table = "v3_dv_dml_compaction";
        createTable(table, "id bigint, v bigint");
        assertUpdate(format("INSERT INTO %s SELECT x, x FROM (%s)", table, range(5000)), 5000);
        assertUpdate("DELETE FROM " + table + " WHERE id % 4 = 0", 1250);
        assertRowLevelUpdate("UPDATE " + table + " SET v = 0 WHERE id % 10 = 1", 500, 500);
        assertTrue(deletionVectorCount(table) > 0);
        assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['rewrite-all'], array['true']))", table, SCHEMA), 3750);
        assertEquals(deletionVectorCount(table), 0L, "compaction must fold deletion vectors into the rewritten data files");
        assertQuery("SELECT count(*), sum(v) FROM " + table,
                "SELECT count(*), sum(CASE WHEN x % 10 = 1 THEN 0 ELSE x END) FROM (" + range(5000) + ") WHERE x % 4 <> 0");
        assertUpdate("DELETE FROM " + table + " WHERE id % 4 = 1", 1250);
        assertQuery("SELECT count(*) FROM " + table, "VALUES 2500");
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testDmlOnBranch()
    {
        String table = "v3_dv_dml_branch";
        createTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        assertUpdate("ALTER TABLE " + table + " CREATE BRANCH 'audit'");
        String branch = format("\"%s.branch_audit\"", table);
        assertRowLevelUpdate("UPDATE " + branch + " SET v = 'B' WHERE id = 2", 1, 1);
        assertUpdate("DELETE FROM " + branch + " WHERE id = 3", 1);
        assertQuery("SELECT id, v FROM " + branch, "VALUES (1, 'a'), (2, 'B')");
        assertQuery("SELECT id, v FROM " + table, "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertEquals(deletionVectorCount(table), 0L, "DML on the branch must not change main");
        dropTable(table);
    }

    @Test
    public void testNoOpDmlWritesNoDeletionVectors()
    {
        String table = "v3_dv_dml_noop";
        createTable(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2)", 2);
        assertUpdate("DELETE FROM " + table + " WHERE id > 100", 0);
        assertUpdate("UPDATE " + table + " SET id = id + 1 WHERE id > 100", 0);
        assertUpdate("MERGE INTO " + table + " t USING (VALUES 9) s(id) ON t.id = s.id WHEN MATCHED THEN DELETE", 0);
        assertEquals(deletionVectorCount(table), 0L);
        assertQuery("SELECT id FROM " + table, "VALUES 1, 2");
        dropTable(table);
    }

    @Test
    public void testJavaWorkerDmlRejectedOnV3()
    {
        // Only native workers write Puffin deletion vectors; Java workers must be rejected and leave the table unchanged.
        String table = "v3_dv_dml_java_rejected";
        createTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')", 2);
        QueryRunner java = (QueryRunner) getExpectedQueryRunner();
        // UPDATE and MERGE are rejected with a native-only error, whose wording varies between versions.
        // DELETE may instead fail inside the Java delete operator with an opaque
        // "com.facebook.presto.common.block.RowBlock" error; drop that alternative once DELETE is
        // rejected with the native-only message too.
        assertJavaFails(java, "DELETE FROM " + table + " WHERE id = 1", NATIVE_ONLY + "|com\\.facebook\\.presto\\.common\\.block\\.RowBlock");
        assertJavaFails(java, "UPDATE " + table + " SET v = 'z' WHERE id = 2", NATIVE_ONLY);
        assertJavaFails(java, "MERGE INTO " + table + " t USING (VALUES 1) s(id) ON t.id = s.id WHEN MATCHED THEN DELETE", NATIVE_ONLY);
        assertQuery("SELECT id, v FROM " + table, "VALUES (1, 'a'), (2, 'b')");
        assertEquals(deletionVectorCount(table), 0L);
        dropTable(table);
    }

    @Test
    public void testEveryOrderingOfThreeOperations()
    {
        // All 27 sequences of three statements drawn from DELETE, UPDATE and MERGE, each on a fresh
        // table, so every operation runs after every other operation and after itself.
        String[] operations = {"D", "U", "M"};
        for (String first : operations) {
            for (String second : operations) {
                for (String third : operations) {
                    String sequence = first + second + third;
                    String table = "v3_dv_seq_" + sequence.toLowerCase(java.util.Locale.ENGLISH);
                    Map<Long, Long> model = seededTable(table, 300);
                    String[] steps = {first, second, third};
                    for (int step = 0; step < steps.length; step++) {
                        applyOperation(table, model, steps[step], step + 1);
                        assertConsistent(table, model, sequence + " step " + (step + 1));
                    }
                    dropTable(table);
                }
            }
        }
    }

    @Test
    public void testAlternatingDeleteAndUpdateOnRewrittenRows()
    {
        // DELETE and UPDATE alternate twelve times. Each UPDATE targets rows that earlier UPDATEs
        // already rewrote, so later deletion vectors land on data files written by earlier UPDATEs.
        String table = "v3_dv_seq_alternate";
        Map<Long, Long> model = seededTable(table, 2000);
        for (int step = 1; step <= 12; step++) {
            if (step % 2 == 1) {
                int modulus = 9 + step;
                int remainder = step % modulus;
                applyDelete(table, model, format("id %% %s = %s", modulus, remainder), id -> id % modulus == remainder);
            }
            else {
                long round = step;
                applyUpdate(table, model, format("v + %s", round), "id % 3 <> 0", id -> id % 3 != 0, value -> value + round);
            }
            assertConsistent(table, model, "step " + step);
        }
        assertTrue(dataFilePaths(table).size() > 6, "every UPDATE writes new data files");
        dropTable(table);
    }

    @Test
    public void testSameRowLifecycle()
    {
        // One row is updated, deleted, re-inserted by MERGE, updated, deleted and re-inserted again.
        String table = "v3_dv_seq_row_lifecycle";
        Map<Long, Long> model = seededTable(table, 50);
        String insertAgain = "MERGE INTO " + table + " t USING (VALUES 25) s(id) ON t.id = s.id WHEN NOT MATCHED THEN INSERT VALUES (s.id, %s)";
        for (int cycle = 1; cycle <= 2; cycle++) {
            long increment = cycle * 10;
            applyUpdate(table, model, format("v + %s", increment), "id = 25", id -> id == 25, value -> value + increment);
            assertConsistent(table, model, "cycle " + cycle + " update");
            applyDelete(table, model, "id = 25", id -> id == 25);
            assertConsistent(table, model, "cycle " + cycle + " delete");
            assertUpdate(format(insertAgain, -cycle), 1);
            model.put(25L, (long) -cycle);
            assertConsistent(table, model, "cycle " + cycle + " reinsert");
        }
        assertQuery("SELECT v FROM " + table + " WHERE id = 25", "VALUES -2");
        dropTable(table);
    }

    @Test
    public void testSequenceMovingRowsAcrossPartitions()
    {
        // Partitioned by truncate(v, 100): an UPDATE of v moves rows into another partition, so each
        // UPDATE adds a deletion vector in the old partition and a data file in the new one.
        String table = "v3_dv_seq_partition_moves";
        Map<Long, Long> model = seededTable(table, 1000, "partitioning = ARRAY['truncate(v, 100)']", "x % 500");
        for (int step = 1; step <= 8; step++) {
            String operation = step % 4 == 0 ? "M" : step % 2 == 1 ? "D" : "U";
            if (operation.equals("U")) {
                long shift = 100L * step;
                int remainder = step % 4;
                applyUpdate(table, model, format("v + %s", shift), format("id %% 4 = %s", remainder), id -> id % 4 == remainder, value -> value + shift);
            }
            else {
                applyOperation(table, model, operation, step);
            }
            assertConsistent(table, model, "step " + step + " " + operation);
        }
        assertTrue(scalarLong("SELECT count(*) FROM \"" + table + "$partitions\"") > 5, "UPDATEs must have moved rows into new partitions");
        dropTable(table);
    }

    @Test
    public void testSequenceOnBucketedTable()
    {
        String table = "v3_dv_seq_bucketed";
        Map<Long, Long> model = seededTable(table, 2000, "partitioning = ARRAY['bucket(id, 8)']", "0");
        for (int step = 1; step <= 9; step++) {
            applyOperation(table, model, new String[] {"D", "U", "M"}[step % 3], step);
            assertConsistent(table, model, "step " + step);
        }
        assertQuery("SELECT count(*) FROM \"" + table + "$partitions\"", "VALUES 8");
        dropTable(table);
    }

    @Test
    public void testSequenceInterleavedWithInserts()
    {
        // New data files arrive between row-level changes; each later change touches old and new files.
        String table = "v3_dv_seq_with_inserts";
        Map<Long, Long> model = seededTable(table, 1000);
        long nextId = 100_000;
        for (int round = 1; round <= 5; round++) {
            assertUpdate(format("INSERT INTO %s SELECT x + %s, %s FROM (%s)", table, nextId, round, range(200)), 200);
            for (long id = nextId + 1; id <= nextId + 200; id++) {
                model.put(id, (long) round);
            }
            nextId += 1000;
            int modulus = 5 + round;
            applyDelete(table, model, format("id %% %s = 0", modulus), id -> id % modulus == 0);
            long increment = round;
            applyUpdate(table, model, format("v + %s", increment), "id % 2 = 1", id -> id % 2 == 1, value -> value + increment);
            assertConsistent(table, model, "round " + round);
        }
        dropTable(table);
    }

    @Test
    public void testSequenceInterleavedWithCompaction()
    {
        // Compaction folds the deletion vectors away; the following DELETE and UPDATE start over on the
        // rewritten files.
        String table = "v3_dv_seq_with_compaction";
        Map<Long, Long> model = seededTable(table, 3000);
        for (int round = 1; round <= 3; round++) {
            applyOperation(table, model, "D", round);
            applyOperation(table, model, "U", round);
            applyOperation(table, model, "M", round);
            assertConsistent(table, model, "round " + round + " before compaction");
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['rewrite-all'], array['true']))", table, SCHEMA),
                    model.size());
            assertEquals(deletionVectorCount(table), 0L, "round " + round + ": compaction must fold the deletion vectors");
            assertConsistent(table, model, "round " + round + " after compaction");
        }
        dropTable(table);
    }

    @Test
    public void testEverySnapshotOfASequenceIsReadable()
    {
        // Each statement's snapshot keeps the deletion vectors of that moment: time travel to any step
        // returns the rows the table had then.
        String table = "v3_dv_seq_snapshots";
        Map<Long, Long> model = seededTable(table, 500);
        List<Long> snapshots = new ArrayList<>();
        List<Map<Long, Long>> models = new ArrayList<>();
        snapshots.add(currentSnapshotId(table));
        models.add(new TreeMap<>(model));
        String[] operations = {"D", "U", "D", "U", "M", "D", "M", "U"};
        for (int step = 0; step < operations.length; step++) {
            applyOperation(table, model, operations[step], step + 1);
            snapshots.add(currentSnapshotId(table));
            models.add(new TreeMap<>(model));
        }
        for (int step = 0; step < snapshots.size(); step++) {
            assertMatchesModel(table + " FOR VERSION AS OF " + snapshots.get(step), models.get(step), "snapshot after step " + step);
        }
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testRollbackInTheMiddleOfASequence()
    {
        // After a rollback, the next statements must replace the deletion vectors of the restored
        // snapshot, not those of the discarded steps.
        String table = "v3_dv_seq_rollback";
        Map<Long, Long> model = seededTable(table, 1000);
        applyOperation(table, model, "D", 1);
        applyOperation(table, model, "U", 2);
        long restored = currentSnapshotId(table);
        Map<Long, Long> restoredModel = new TreeMap<>(model);
        applyOperation(table, model, "D", 3);
        applyOperation(table, model, "U", 4);
        applyOperation(table, model, "M", 5);
        assertUpdate(format("CALL system.rollback_to_snapshot('%s', '%s', %s)", SCHEMA, table, restored));
        model = restoredModel;
        assertConsistent(table, model, "after rollback");
        for (int step = 6; step <= 9; step++) {
            applyOperation(table, model, new String[] {"D", "U", "M"}[step % 3], step);
            assertConsistent(table, model, "step " + step + " after rollback");
        }
        dropTable(table);
    }

    @Test
    public void testSequenceAcrossSchemaChanges()
    {
        // Columns are added, filled, renamed and dropped between row-level changes, so deletion vectors
        // apply to data files written under several schemas.
        String table = "v3_dv_seq_schema";
        Map<Long, Long> model = seededTable(table, 600);
        applyOperation(table, model, "D", 1);
        assertUpdate("ALTER TABLE " + table + " ADD COLUMN w bigint");
        assertRowLevelUpdate("UPDATE " + table + " SET w = id WHERE id % 2 = 0", matching(model, id -> id % 2 == 0).size(), matching(model, id -> id % 2 == 0).size());
        applyOperation(table, model, "U", 2);
        assertUpdate("ALTER TABLE " + table + " RENAME COLUMN w TO w2");
        applyDelete(table, model, "w2 IS NOT NULL AND w2 % 3 = 0", id -> id % 2 == 0 && id % 3 == 0);
        assertEquals(scalarLong("SELECT count(w2) FROM " + table), (long) matching(model, id -> id % 2 == 0).size());
        assertUpdate("ALTER TABLE " + table + " DROP COLUMN w2");
        applyOperation(table, model, "M", 3);
        applyOperation(table, model, "U", 4);
        assertConsistent(table, model, "after schema changes");
        dropTable(table);
    }

    @Test
    public void testSequencesOnBranchAndMainStayIndependent()
    {
        String table = "v3_dv_seq_branch";
        Map<Long, Long> main = seededTable(table, 500);
        assertUpdate("ALTER TABLE " + table + " CREATE BRANCH 'dev'");
        Map<Long, Long> dev = new TreeMap<>(main);
        String branch = format("\"%s.branch_dev\"", table);
        for (int step = 1; step <= 6; step++) {
            applyOperation(branch, dev, new String[] {"D", "U", "M"}[step % 3], step);
            applyOperation(table, main, new String[] {"U", "D", "M"}[step % 3], step + 10);
            assertMatchesModel(branch, dev, "branch step " + step);
            assertConsistent(table, main, "main step " + step);
        }
        dropTable(table);
    }

    @Test
    public void testSequenceOnLargeFileReadInManySplits()
    {
        // One 200,000-row data file read through 64 KB splits: every statement reads the file from
        // several tasks, each writing a deletion vector for it, which Iceberg merges at commit.
        String table = "v3_dv_seq_large_file";
        createTable(table, "id bigint, v bigint");
        assertUpdate(com.facebook.presto.Session.builder(getSession()).setSystemProperty("task_writer_count", "1").build(),
                format("INSERT INTO %s SELECT x, 0 FROM (%s)", table, range(200_000)), 200_000);
        assertEquals(dataFilePaths(table).size(), 1);
        assertUpdate(format("ALTER TABLE %s SET PROPERTIES (\"read.split.target-size\" = 65536)", table));
        Map<Long, Long> model = new TreeMap<>();
        for (long id = 1; id <= 200_000; id++) {
            model.put(id, 0L);
        }
        for (int step = 1; step <= 6; step++) {
            if (step % 2 == 1) {
                int modulus = 10 + step;
                applyDelete(table, model, format("id %% %s = 1", modulus), id -> id % modulus == 1);
            }
            else {
                long increment = step;
                int remainder = step % 7;
                applyUpdate(table, model, format("v + %s", increment), format("id %% 7 = %s", remainder), id -> id % 7 == remainder, value -> value + increment);
            }
            assertConsistent(table, model, "step " + step);
        }
        dropTable(table);
    }

    // Creates (id bigint, v bigint) with rows 1..rows and v = 0, and returns the matching model.
    private Map<Long, Long> seededTable(String table, int rows)
    {
        return seededTable(table, rows, null, "0");
    }

    private Map<Long, Long> seededTable(String table, int rows, String extraProperties, String valueExpression)
    {
        createTable(table, "id bigint, v bigint", extraProperties);
        assertUpdate(format("INSERT INTO %s SELECT x, %s FROM (%s)", table, valueExpression, range(rows)), rows);
        Map<Long, Long> model = new TreeMap<>();
        for (MaterializedRow row : computeActual("SELECT id, v FROM " + table).getMaterializedRows()) {
            model.put(((Number) row.getField(0)).longValue(), ((Number) row.getField(1)).longValue());
        }
        assertEquals(model.size(), rows);
        return model;
    }

    // D deletes, U updates and M merges (delete, update and insert) overlapping rows chosen by step.
    private void applyOperation(String table, Map<Long, Long> model, String operation, int step)
    {
        switch (operation) {
            case "D": {
                int modulus = 6 + step % 5;
                int remainder = step % modulus;
                applyDelete(table, model, format("id %% %s = %s", modulus, remainder), id -> id % modulus == remainder);
                break;
            }
            case "U": {
                long increment = 100L * step + 1;
                int remainder = step % 4;
                applyUpdate(table, model, format("v + %s", increment), format("id %% 4 = %s", remainder), id -> id % 4 == remainder, value -> value + increment);
                break;
            }
            case "M": {
                long low = 1 + 17L * step;
                long insertFrom = 1_000_000L + 100L * step;
                applyMerge(table, model, low, low + 60, insertFrom, insertFrom + 4, step);
                break;
            }
            default:
                throw new IllegalArgumentException(operation);
        }
    }

    // Rows match the model, the V3 delete invariants hold, and table metadata agrees with the rows read:
    // records in the data files minus rows deleted by deletion vectors equals the live row count.
    private void assertConsistent(String table, Map<Long, Long> model, String context)
    {
        assertMatchesModel(table, model, context);
        assertDeletionVectorInvariants(table);
        assertEquals(liveRowCountFromMetadata(table), model.size(), context + ": data-file records minus deletion-vector rows");
    }

    // Writes one DV per data file with the Iceberg Java API and commits them; returns the deleted row count.
    private long writeDeletionVectorsWithIcebergApi(String table, LongPredicate deletePosition)
            throws IOException
    {
        Table icebergTable = loadTable(table);
        List<DataFile> files = new ArrayList<>();
        fileScanTasks(table).forEach(task -> files.add(task.file()));
        OutputFileFactory puffinFiles = OutputFileFactory.builderFor(icebergTable, 1, System.nanoTime()).format(FileFormat.PUFFIN).build();
        DVFileWriter writer = new BaseDVFileWriter(puffinFiles, path -> null);
        long deleted = 0;
        try (DVFileWriter closing = writer) {
            for (DataFile file : files) {
                for (long position = 0; position < file.recordCount(); position++) {
                    if (deletePosition.test(position)) {
                        closing.delete(file.location(), position, icebergTable.specs().get(file.specId()), file.partition());
                        deleted++;
                    }
                }
            }
        }
        RowDelta rowDelta = icebergTable.newRowDelta();
        writer.result().deleteFiles().forEach(rowDelta::addDeletes);
        rowDelta.commit();
        return deleted;
    }

    private long liveRowCountFromMetadata(String table)
    {
        return fileScanTasks(table).stream().mapToLong(task -> task.file().recordCount()).sum() - deletedRowsInDeletionVectors(table);
    }

    @Test
    public void testNativeDmlOnDeletionVectorsWrittenByAnotherEngine()
            throws IOException
    {
        // DVs written through the Iceberg Java API, the writer other engines such as Spark use, must be
        // applied by native reads, and a later native DELETE or UPDATE must replace them with a DV that
        // still covers the positions the other engine deleted.
        String table = "v3_dv_external_writer";
        createTable(table, "id bigint, v bigint");
        assertUpdate(format("INSERT INTO %s SELECT x, 0 FROM (%s)", table, range(3000)), 3000);
        long dataFiles = dataFilePaths(table).size();

        long externallyDeleted = writeDeletionVectorsWithIcebergApi(table, position -> position % 10 == 3);
        assertEquals(deletionVectorCount(table), dataFiles, "one externally written DV per data file");
        assertEquals(scalarLong("SELECT count(*) FROM " + table), 3000 - externallyDeleted);
        assertEquals(liveRowCountFromMetadata(table), 3000 - externallyDeleted);
        assertDeletionVectorInvariants(table);

        long nativelyDeleted = scalarLong("SELECT count(*) FROM " + table + " WHERE id % 7 = 0");
        assertUpdate("DELETE FROM " + table + " WHERE id % 7 = 0", nativelyDeleted);
        assertEquals(deletedRowsInDeletionVectors(table), externallyDeleted + nativelyDeleted, "the native DV must keep the externally deleted positions");
        assertEquals(scalarLong("SELECT count(*) FROM " + table), 3000 - externallyDeleted - nativelyDeleted);
        assertTrue(deletionVectorCount(table) <= dataFiles);
        assertDeletionVectorInvariants(table);

        long updated = scalarLong("SELECT count(*) FROM " + table + " WHERE id % 5 = 1");
        assertRowLevelUpdate("UPDATE " + table + " SET v = 1 WHERE id % 5 = 1", updated, updated);
        long live = 3000 - externallyDeleted - nativelyDeleted;
        assertEquals(scalarLong("SELECT count(*) FROM " + table), live);
        assertEquals(scalarLong("SELECT count_if(v = 1) FROM " + table), updated);
        assertEquals(liveRowCountFromMetadata(table), live);
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    @Test
    public void testDeletionVectorBitmapEdgePositions()
            throws IOException
    {
        // A DV is a Roaring bitmap with one container per 65,536 positions. Delete the first and last row
        // of a file, a contiguous run, and a run across the container boundary, then delete natively on top.
        String table = "v3_dv_bitmap_edges";
        createTable(table, "id bigint, v bigint");
        assertUpdate(com.facebook.presto.Session.builder(getSession()).setSystemProperty("task_writer_count", "1").build(),
                format("INSERT INTO %s SELECT x, 0 FROM (%s)", table, range(140_000)), 140_000);
        assertEquals(dataFilePaths(table).size(), 1);
        long last = fileScanTasks(table).get(0).file().recordCount() - 1;

        long deleted = writeDeletionVectorsWithIcebergApi(table, position -> position == 0 || position == last
                || (position >= 1000 && position < 2000)
                || (position >= 65_530 && position < 65_540)
                || (position >= 131_068 && position < 131_076));
        assertEquals(deleted, 2 + 1000 + 10 + 8);
        assertEquals(scalarLong("SELECT count(*) FROM " + table), 140_000 - deleted);
        assertDeletionVectorInvariants(table);

        long nativelyDeleted = scalarLong("SELECT count(*) FROM " + table + " WHERE id BETWEEN 65000 AND 66000 OR id > 139990");
        assertUpdate("DELETE FROM " + table + " WHERE id BETWEEN 65000 AND 66000 OR id > 139990", nativelyDeleted);
        assertEquals(deletedRowsInDeletionVectors(table), deleted + nativelyDeleted);
        assertEquals(scalarLong("SELECT count(*) FROM " + table), 140_000 - deleted - nativelyDeleted);
        assertEquals(deletionVectorCount(table), 1L);
        assertDeletionVectorInvariants(table);
        dropTable(table);
    }

    private static void assertJavaFails(QueryRunner java, String sql, String expectedMessagePattern)
    {
        try {
            java.execute(sql);
        }
        catch (RuntimeException e) {
            assertTrue(Pattern.compile(expectedMessagePattern).matcher(String.valueOf(e.getMessage())).find(), sql + ": " + e.getMessage());
            return;
        }
        throw new AssertionError("Java worker DML on a V3 table must fail: " + sql);
    }

    private void applyDelete(String table, Map<Long, Long> model, String predicate, LongPredicate matches)
    {
        List<Long> matched = matching(model, matches);
        assertUpdate(format("DELETE FROM %s WHERE %s", table, predicate), matched.size());
        matched.forEach(model::remove);
    }

    private void applyUpdate(String table, Map<Long, Long> model, String newValue, String predicate, LongPredicate matches, LongUnaryOperator update)
    {
        List<Long> matched = matching(model, matches);
        assertRowLevelUpdate(format("UPDATE %s SET v = %s WHERE %s", table, newValue, predicate), matched.size(), matched.size());
        matched.forEach(id -> model.put(id, update.applyAsLong(model.get(id))));
    }

    // Source ids low..high delete matched even ids and double the rest; ids insertFrom..insertTo are new.
    private void applyMerge(String table, Map<Long, Long> model, long low, long high, long insertFrom, long insertTo, int round)
    {
        long updated = 0;
        long changed = 0;
        Set<Long> sourceIds = new HashSet<>();
        for (long id = low; id <= high; id++) {
            sourceIds.add(id);
        }
        for (long id = insertFrom; id <= insertTo; id++) {
            sourceIds.add(id);
        }
        for (long id : sourceIds) {
            if (model.containsKey(id)) {
                changed++;
                if (id % 2 == 0) {
                    model.remove(id);
                }
                else {
                    updated++;
                    model.put(id, model.get(id) * 2 + round);
                }
            }
            else if (id >= insertFrom) {
                changed++;
                model.put(id, (long) -round);
            }
        }
        assertRowLevelUpdate(format("MERGE INTO %1$s t USING (SELECT x AS id FROM UNNEST(sequence(%2$s, %3$s)) AS s(x) UNION ALL SELECT x FROM UNNEST(sequence(%4$s, %5$s)) AS s(x)) s "
                        + "ON t.id = s.id "
                        + "WHEN MATCHED AND s.id %% 2 = 0 THEN DELETE "
                        + "WHEN MATCHED THEN UPDATE SET v = t.v * 2 + %6$s "
                        + "WHEN NOT MATCHED AND s.id >= %4$s THEN INSERT VALUES (s.id, -%6$s)",
                table, low, high, insertFrom, insertTo, round), changed, updated);
    }

    private static List<Long> matching(Map<Long, Long> model, LongPredicate matches)
    {
        List<Long> matched = new ArrayList<>();
        for (long id : model.keySet()) {
            if (matches.test(id)) {
                matched.add(id);
            }
        }
        return matched;
    }

    private void assertMatchesModel(String table, Map<Long, Long> model, String context)
    {
        long sumIds = 0;
        long sumValues = 0;
        long weighted = 0;
        for (Map.Entry<Long, Long> entry : model.entrySet()) {
            sumIds += entry.getKey();
            sumValues += entry.getValue();
            weighted += entry.getKey() * (entry.getValue() + 1);
        }
        MaterializedRow row = getOnlyElement(computeActual(format(
                "SELECT count(*), coalesce(sum(id), 0), coalesce(sum(v), 0), coalesce(sum(id * (v + 1)), 0), count(DISTINCT id) FROM %s", table))
                .getMaterializedRows());
        assertEquals(((Number) row.getField(0)).longValue(), model.size(), context + ": row count");
        assertEquals(((Number) row.getField(1)).longValue(), sumIds, context + ": sum(id)");
        assertEquals(((Number) row.getField(2)).longValue(), sumValues, context + ": sum(v)");
        assertEquals(((Number) row.getField(3)).longValue(), weighted, context + ": sum(id * (v + 1))");
        assertEquals(((Number) row.getField(4)).longValue(), model.size(), context + ": ids must stay unique");
    }

    /**
     * Runs an UPDATE or MERGE and checks its reported row count. Native workers built against a Velox
     * without facebookincubator/velox#19706 count each updated row twice (its delete and its re-insert),
     * so the count may also be {@code expectedRows + updatedRows}. The table contents are checked separately.
     */
    private void assertRowLevelUpdate(String sql, long expectedRows, long updatedRows)
    {
        long actual = getQueryRunner().execute(getSession(), sql).getUpdateCount().getAsLong();
        assertTrue(actual == expectedRows || actual == expectedRows + updatedRows,
                format("%s: expected %s rows (or %s before velox#19706) but got %s", sql, expectedRows, expectedRows + updatedRows, actual));
    }

    private void createTable(String table, String columns)
    {
        createTable(table, columns, null);
    }

    private void createTable(String table, String columns, String extraProperties)
    {
        dropTable(table);
        assertUpdate(format("CREATE TABLE %s (%s) WITH (\"format-version\" = '3'%s)", table, columns, extraProperties == null ? "" : ", " + extraProperties));
    }

    private void dropTable(String table)
    {
        assertUpdate("DROP TABLE IF EXISTS " + table);
    }

    private static String range(long count)
    {
        if (count <= 10_000) {
            return format("SELECT CAST(x AS bigint) AS x FROM UNNEST(sequence(1, %s)) AS t(x)", count);
        }
        return format("SELECT CAST(a * 1000 + b AS bigint) AS x FROM UNNEST(sequence(0, %s)) AS t(a) CROSS JOIN UNNEST(sequence(1, 1000)) AS u(b)", count / 1000 - 1);
    }

    private long scalarLong(String sql)
    {
        return ((Number) computeScalar(sql)).longValue();
    }

    private long currentSnapshotId(String table)
    {
        return loadTable(table).currentSnapshot().snapshotId();
    }

    private List<String> dataFilePaths(String table)
    {
        List<String> paths = new ArrayList<>();
        for (FileScanTask task : fileScanTasks(table)) {
            paths.add(task.file().location());
        }
        paths.sort(String::compareTo);
        return paths;
    }

    private long deletionVectorCount(String table)
    {
        return liveDeletionVectors(table).size();
    }

    private long deletionVectorCountWhere(String table, java.util.function.Predicate<String> referencedDataFile)
    {
        return liveDeletionVectors(table).stream().filter(delete -> referencedDataFile.test(String.valueOf(delete.referencedDataFile()))).count();
    }

    private long deletedRowsInDeletionVectors(String table)
    {
        return liveDeletionVectors(table).stream().mapToLong(DeleteFile::recordCount).sum();
    }

    // Deletion vectors of the current snapshot; several can share one Puffin file at different offsets.
    private List<DeleteFile> liveDeletionVectors(String table)
    {
        Map<String, DeleteFile> deletionVectors = new LinkedHashMap<>();
        for (FileScanTask task : fileScanTasks(table)) {
            for (DeleteFile delete : task.deletes()) {
                if (isDeletionVector(delete)) {
                    deletionVectors.putIfAbsent(delete.location() + "#" + delete.contentOffset(), delete);
                }
            }
        }
        return new ArrayList<>(deletionVectors.values());
    }

    private void assertDeletionVectorInvariants(String table)
    {
        for (FileScanTask task : fileScanTasks(table)) {
            DataFile file = task.file();
            long deletionVectors = 0;
            for (DeleteFile delete : task.deletes()) {
                if (delete.content() == FileContent.POSITION_DELETES) {
                    assertTrue(isDeletionVector(delete), "V3 position deletes must be Puffin deletion vectors: " + delete.location());
                    assertEquals(String.valueOf(delete.referencedDataFile()), file.location(), "deletion vector must reference its data file");
                    assertTrue(delete.recordCount() <= file.recordCount(), "deletion vector deletes more rows than its data file holds: " + file.location());
                    deletionVectors++;
                }
            }
            assertTrue(deletionVectors <= 1, format("data file %s has %s deletion vectors; V3 allows at most one", file.location(), deletionVectors));
        }
    }

    private static boolean isDeletionVector(DeleteFile delete)
    {
        return delete.content() == FileContent.POSITION_DELETES && delete.format() == FileFormat.PUFFIN;
    }

    private List<FileScanTask> fileScanTasks(String table)
    {
        try (CloseableIterable<FileScanTask> tasks = loadTable(table).newScan().planFiles()) {
            return ImmutableList.copyOf(tasks);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Table loadTable(String table)
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = IcebergQueryRunner.getIcebergDataDirectoryPath(dataDirectory, CatalogType.HADOOP.name(), new IcebergConfig().getFileFormat(), true);
        return new HadoopCatalog(new Configuration(false), catalogDirectory.toString()).loadTable(TableIdentifier.of(SCHEMA, table));
    }
}
