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

import com.facebook.presto.testing.MaterializedRow;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Repeated DELETE, UPDATE and MERGE rounds over the same native-written data files, each round
 * checked through native reads against an in-memory model of the table. The mutations run on Java
 * workers, which write position-delete files, because native row-level DML is still being
 * implemented (see {@link AbstractTestNativeIcebergV3Dml}). Tables are V2 merge-on-read.
 */
public abstract class AbstractTestNativeIcebergV3DeleteStress
        extends AbstractNativeIcebergV3Test
{
    private static final long SEED = 20261020L;

    protected AbstractTestNativeIcebergV3DeleteStress(String storageFormat)
    {
        super(storageFormat);
    }

    @Test
    public void testDeleteUpdateDeleteRounds()
    {
        String table = "stress_rounds";
        int rows = 20_000;
        createV2MergeOnReadTable(table, "id bigint, v bigint");
        assertUpdate(format("INSERT INTO %s SELECT x, 0 FROM (%s)", table, rangeOneTo(rows)), rows);
        long dataFilesBeforeMutations = dataFileCount(table);

        Map<Long, Long> model = new TreeMap<>();
        for (long id = 1; id <= rows; id++) {
            model.put(id, 0L);
        }

        Random random = new Random(SEED);
        for (int round = 1; round <= 12; round++) {
            int deleteModulus = 7 + random.nextInt(30);
            int deleteRemainder = random.nextInt(deleteModulus);
            mutate(model, format("DELETE FROM %s WHERE id %% %s = %s", table, deleteModulus, deleteRemainder),
                    id -> id % deleteModulus == deleteRemainder, null);

            int updateModulus = 3 + random.nextInt(10);
            int updateRemainder = random.nextInt(updateModulus);
            long increment = round;
            mutate(model, format("UPDATE %s SET v = v + %s WHERE id %% %s = %s", table, increment, updateModulus, updateRemainder),
                    id -> id % updateModulus == updateRemainder, value -> value + increment);

            long low = 1 + random.nextInt(rows - 500);
            long high = low + random.nextInt(400);
            mutate(model, format("DELETE FROM %s WHERE id BETWEEN %s AND %s", table, low, high),
                    id -> id >= low && id <= high, null);

            assertTableMatchesModel(table, model);
        }
        assertTrue(dataFileCount(table) > dataFilesBeforeMutations, "UPDATEs must add data files");
        dropTable(table);
    }

    @Test
    public void testMergeRounds()
    {
        String table = "stress_merge_rounds";
        createV2MergeOnReadTable(table, "id bigint, v bigint");
        assertUpdate(format("INSERT INTO %s SELECT x, x FROM (%s)", table, rangeOneTo(5000)), 5000);
        Map<Long, Long> model = new TreeMap<>();
        for (long id = 1; id <= 5000; id++) {
            model.put(id, id);
        }

        for (int round = 1; round <= 8; round++) {
            long start = round * 400L;
            long end = start + 1200;
            long r = round;
            long changed = 0;
            for (long id = start; id <= end; id++) {
                if (model.containsKey(id)) {
                    changed++;
                    if (id % 5 == r % 5) {
                        model.remove(id);
                    }
                    else {
                        model.put(id, model.get(id) * 2);
                    }
                }
                else if (id % 2 == 0) {
                    changed++;
                    model.put(id, -id);
                }
            }
            assertJavaUpdate(format("MERGE INTO %s t USING (SELECT x AS id FROM UNNEST(sequence(%s, %s)) AS s(x)) s ON t.id = s.id "
                    + "WHEN MATCHED AND s.id %% 5 = %s THEN DELETE "
                    + "WHEN MATCHED THEN UPDATE SET v = t.v * 2 "
                    + "WHEN NOT MATCHED AND s.id %% 2 = 0 THEN INSERT VALUES (s.id, -s.id)", table, start, end, r % 5), changed);
            assertTableMatchesModel(table, model);
        }
        dropTable(table);
    }

    @Test
    public void testLargeDelete()
    {
        String table = "stress_large";
        int rows = 100_000;
        createV2MergeOnReadTable(table, "id bigint, payload varchar");
        assertUpdate(format("INSERT INTO %s SELECT x, 'p' || CAST(x AS varchar) FROM (%s)", table, rangeOneTo(rows)), rows);

        // A sparse pass, then a contiguous range, then a second sparse pass.
        assertJavaUpdate("DELETE FROM " + table + " WHERE id % 2 = 0", 50_000);
        assertJavaUpdate("DELETE FROM " + table + " WHERE id BETWEEN 20001 AND 60000", 20_000);
        assertJavaUpdate("DELETE FROM " + table + " WHERE id % 3 = 0", 10_000);

        long expected = 0;
        long expectedSum = 0;
        for (long id = 1; id <= rows; id++) {
            if (id % 2 == 1 && (id < 20001 || id > 60000) && id % 3 != 0) {
                expected++;
                expectedSum += id;
            }
        }
        assertQuery("SELECT count(*), sum(id) FROM " + table, format("VALUES (%s, %s)", expected, expectedSum));
        assertQuery("SELECT count(*) FROM " + table + " WHERE id % 2 = 0 OR id BETWEEN 20001 AND 60000 OR id % 3 = 0", "VALUES 0");
        assertQuery("SELECT payload FROM " + table + " WHERE id IN (99997, 99999)", "VALUES 'p99997'");
        assertEquals(positionDeleteRowCount(table), rows - expected);
        dropTable(table);
    }

    @Test
    public void testManySmallFiles()
    {
        String table = "stress_small_files";
        int files = 20;
        createV2MergeOnReadTable(table, "id integer, file integer");
        for (int file = 0; file < files; file++) {
            assertUpdate(format("INSERT INTO %s VALUES (%s, %s), (%s, %s), (%s, %s), (%s, %s)", table,
                    file * 4, file, file * 4 + 1, file, file * 4 + 2, file, file * 4 + 3, file), 4);
        }
        assertTrue(dataFileCount(table) >= files, "expected at least one data file per INSERT");

        // Three passes, each touching every data file again.
        for (int pass = 0; pass < 3; pass++) {
            assertJavaUpdate(format("DELETE FROM %s WHERE id %% 4 = %s", table, pass), files);
            assertEquals(positionDeleteRowCount(table), (long) files * (pass + 1));
        }
        assertQuery("SELECT count(*), min(id % 4), max(id % 4) FROM " + table, format("VALUES (%s, 3, 3)", files));
        dropTable(table);
    }

    @Test
    public void testDeleteEveryRowOfFile()
    {
        String table = "stress_full_file";
        createV2MergeOnReadTable(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2), (3)", 3);
        assertUpdate("INSERT INTO " + table + " VALUES (4), (5), (6)", 3);
        assertJavaUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        assertJavaUpdate("DELETE FROM " + table + " WHERE id IN (2, 3)", 2);
        assertQuery("SELECT id FROM " + table, "VALUES 4, 5, 6");
        assertJavaUpdate("UPDATE " + table + " SET id = id * 10 WHERE id = 5", 1);
        assertQuery("SELECT id FROM " + table, "VALUES 4, 6, 50");
        dropTable(table);
    }

    @Test
    public void testRewriteDataFilesAppliesDeletes()
    {
        String table = "stress_rewrite";
        createV2MergeOnReadTable(table, "id bigint, v bigint");
        assertUpdate(format("INSERT INTO %s SELECT x, x FROM (%s)", table, rangeOneTo(5000)), 5000);
        assertJavaUpdate("DELETE FROM " + table + " WHERE id % 4 = 0", 1250);
        assertJavaUpdate("UPDATE " + table + " SET v = 0 WHERE id % 10 = 1", 500);
        assertTrue(positionDeleteFileCount(table) > 0);

        assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['rewrite-all'], array['true']))", table, SCHEMA), 3750);
        assertEquals(positionDeleteFileCount(table), 0L, "compaction must fold delete files into rewritten data files");
        assertQuery("SELECT count(*), sum(id), sum(v) FROM " + table,
                "SELECT count(*), sum(x), sum(CASE WHEN x % 10 = 1 THEN 0 ELSE x END) FROM (" + rangeOneTo(5000) + ") WHERE x % 4 <> 0");

        // Row-level DML keeps working on the compacted files.
        assertJavaUpdate("DELETE FROM " + table + " WHERE id % 4 = 1", 1250);
        assertQuery("SELECT count(*) FROM " + table, "VALUES 2500");
        dropTable(table);
    }

    @Test
    public void testTimeTravelAcrossDeletes()
    {
        String table = "stress_time_travel";
        createV2MergeOnReadTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", 4);
        long afterInsert = currentSnapshotId(table);
        assertJavaUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        long afterFirstDelete = currentSnapshotId(table);
        assertJavaUpdate("UPDATE " + table + " SET v = 'X' WHERE id = 2", 1);
        long afterUpdate = currentSnapshotId(table);
        assertJavaUpdate("DELETE FROM " + table + " WHERE id = 3", 1);

        assertQuery("SELECT id, v FROM " + table + " FOR VERSION AS OF " + afterInsert, "VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
        assertQuery("SELECT id, v FROM " + table + " FOR VERSION AS OF " + afterFirstDelete, "VALUES (2, 'b'), (3, 'c'), (4, 'd')");
        assertQuery("SELECT id, v FROM " + table + " FOR VERSION AS OF " + afterUpdate, "VALUES (2, 'X'), (3, 'c'), (4, 'd')");
        assertQuery("SELECT id, v FROM " + table, "VALUES (2, 'X'), (4, 'd')");
        dropTable(table);
    }

    @Test
    public void testRollbackThenMutateAgain()
    {
        String table = "stress_rollback";
        createV2MergeOnReadTable(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2), (3), (4), (5)", 5);
        assertJavaUpdate("DELETE FROM " + table + " WHERE id = 1", 1);
        long afterFirstDelete = currentSnapshotId(table);
        assertJavaUpdate("DELETE FROM " + table + " WHERE id = 2", 1);
        assertQuery("SELECT id FROM " + table, "VALUES 3, 4, 5");

        assertUpdate(format("CALL system.rollback_to_snapshot('%s', '%s', %s)", SCHEMA, table, afterFirstDelete));
        assertQuery("SELECT id FROM " + table, "VALUES 2, 3, 4, 5");
        assertJavaUpdate("DELETE FROM " + table + " WHERE id = 5", 1);
        assertQuery("SELECT id FROM " + table, "VALUES 2, 3, 4");
        assertEquals(positionDeleteRowCount(table), 2L);
        dropTable(table);
    }

    @Test
    public void testExpireSnapshotsAfterDeletes()
    {
        String table = "stress_expire";
        createV2MergeOnReadTable(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2), (3), (4), (5), (6)", 6);
        for (int id = 1; id <= 4; id++) {
            assertJavaUpdate("DELETE FROM " + table + " WHERE id = " + id, 1);
        }
        assertEquals(snapshotCount(table), 5L);

        assertUpdate(format("CALL system.expire_snapshots(schema => '%s', table_name => '%s', older_than => localtimestamp, retain_last => 1)", SCHEMA, table));
        assertEquals(snapshotCount(table), 1L);
        assertQuery("SELECT id FROM " + table, "VALUES 5, 6");
        assertJavaUpdate("DELETE FROM " + table + " WHERE id = 5", 1);
        assertQuery("SELECT id FROM " + table, "VALUES 6");
        dropTable(table);
    }

    private void mutate(Map<Long, Long> model, String sql, LongPredicate matches, LongUnaryOperator update)
    {
        List<Long> matched = new ArrayList<>();
        for (long id : model.keySet()) {
            if (matches.test(id)) {
                matched.add(id);
            }
        }
        assertJavaUpdate(sql, matched.size());
        for (long id : matched) {
            if (update == null) {
                model.remove(id);
            }
            else {
                model.put(id, update.applyAsLong(model.get(id)));
            }
        }
    }

    private void assertTableMatchesModel(String table, Map<Long, Long> model)
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
        assertEquals(((Number) row.getField(0)).longValue(), model.size(), "row count");
        assertEquals(((Number) row.getField(1)).longValue(), sumIds, "sum(id)");
        assertEquals(((Number) row.getField(2)).longValue(), sumValues, "sum(v)");
        assertEquals(((Number) row.getField(3)).longValue(), weighted, "sum(id * (v + 1))");
        assertEquals(((Number) row.getField(4)).longValue(), model.size(), "ids must stay unique");
    }
}
