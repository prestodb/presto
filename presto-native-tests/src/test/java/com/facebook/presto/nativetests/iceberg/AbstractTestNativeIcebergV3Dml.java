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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * INSERT, DELETE, UPDATE and MERGE executed on native workers, on V2 merge-on-read tables.
 *
 * <p>Native row-level DML that writes delete files is still being implemented. Those statements
 * are checked with {@link #assertNativeRowLevelDml}: each one either commits with the right row
 * count and result, or fails and leaves the table unchanged. MERGE statements that only insert
 * must succeed. Once native row-level DML is complete, switch these to plain assertions and run
 * the scenarios exercised through Java workers in {@link AbstractTestNativeIcebergV3DeleteStress}
 * on native workers.
 */
public abstract class AbstractTestNativeIcebergV3Dml
        extends AbstractNativeIcebergV3Test
{
    protected AbstractTestNativeIcebergV3Dml(String storageFormat)
    {
        super(storageFormat);
    }

    @Override
    protected void createTables()
    {
        super.createTables();
        createTpchTables("nation");
    }

    @Test
    public void testInsertValuesAndSelect()
    {
        String table = "dml_insert";
        createV3Table(table, "id integer, name varchar, amount double");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a', 10.0), (2, 'b', 20.0), (3, 'c', 30.0)", 3);
        assertUpdate("INSERT INTO " + table + " SELECT id + 3, name, amount + 30 FROM (VALUES (1, 'd', 10.0), (2, 'e', 20.0)) t(id, name, amount)", 2);
        assertUpdate("INSERT INTO " + table + " (id, name) VALUES (6, 'f')", 1);
        assertQuery("SELECT * FROM " + table, "VALUES (1, 'a', 10.0), (2, 'b', 20.0), (3, 'c', 30.0), (4, 'd', 40.0), (5, 'e', 50.0), (6, 'f', NULL)");
        assertEquals(snapshotCount(table), 3L);
        assertEquals(currentSnapshotOperation(table), "append");
        assertDataFilesUseFormat(table);
        dropTable(table);
    }

    @Test
    public void testInsertSelect()
    {
        String table = "dml_insert_select";
        createV3Table(table, "nationkey bigint, doubled bigint, label varchar");
        assertUpdate("INSERT INTO " + table + " SELECT nationkey, nationkey * 2, upper(name) FROM nation", 25);
        assertQuery("SELECT * FROM " + table, "SELECT nationkey, nationkey * 2, upper(name) FROM nation");
        dropTable(table);
    }

    @Test
    public void testInsertIntoPartitionedTable()
    {
        String table = "dml_insert_partitioned";
        createV3Table(table, "id integer, ds varchar, v integer", "partitioning = ARRAY['ds']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, '2024-01-01', 10), (2, '2024-01-02', 20)", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (3, '2024-01-01', 30), (4, NULL, 40)", 2);
        assertQuery("SELECT ds, count(*), sum(v) FROM " + table + " GROUP BY ds", "VALUES ('2024-01-01', 2, 40), ('2024-01-02', 1, 20), (NULL, 1, 40)");
        dropTable(table);
    }

    @Test
    public void testNativeDelete()
    {
        String table = "dml_native_delete";
        createV2MergeOnReadTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        assertNativeRowLevelDml("DELETE FROM " + table + " WHERE id = 2", 1, table, "VALUES (1, 'a'), (3, 'c')", "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        dropTable(table);
    }

    @Test
    public void testNativeUpdate()
    {
        String table = "dml_native_update";
        createV2MergeOnReadTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        assertNativeRowLevelDml("UPDATE " + table + " SET v = 'z' WHERE id = 2", 1, table, "VALUES (1, 'a'), (2, 'z'), (3, 'c')", "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        dropTable(table);
    }

    @Test
    public void testNativeMergeWithMatchedClause()
    {
        String original = "VALUES (1, 'a'), (2, 'b'), (3, 'c')";
        String upsert = "dml_native_merge_upsert";
        createV2MergeOnReadTable(upsert, "id integer, v varchar");
        assertUpdate("INSERT INTO " + upsert + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        assertNativeRowLevelDml("MERGE INTO " + upsert + " t USING (VALUES (2, 'B'), (4, 'D')) s(id, v) ON t.id = s.id "
                        + "WHEN MATCHED THEN UPDATE SET v = s.v "
                        + "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
                2, upsert, "VALUES (1, 'a'), (2, 'B'), (3, 'c'), (4, 'D')", original);
        dropTable(upsert);

        String delete = "dml_native_merge_delete";
        createV2MergeOnReadTable(delete, "id integer, v varchar");
        assertUpdate("INSERT INTO " + delete + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        assertNativeRowLevelDml("MERGE INTO " + delete + " t USING (VALUES 3) s(id) ON t.id = s.id WHEN MATCHED THEN DELETE",
                1, delete, "VALUES (1, 'a'), (2, 'b')", original);
        dropTable(delete);
    }

    @Test
    public void testNativeMergeInsertOnly()
    {
        String table = "dml_merge_insert_only";
        createV2MergeOnReadTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a')", 1);
        assertUpdate("MERGE INTO " + table + " t USING (VALUES (1, 'x'), (2, 'b'), (3, 'c')) s(id, v) ON t.id = s.id "
                + "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)", 2);
        assertQuery("SELECT id, v FROM " + table, "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertEquals(positionDeleteFileCount(table), 0L, "an insert-only MERGE must not write delete files");
        dropTable(table);
    }

    @Test
    public void testNativeMergeNoMatchingRows()
    {
        String table = "dml_merge_noop";
        createV2MergeOnReadTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a')", 1);
        assertUpdate("MERGE INTO " + table + " t USING (VALUES (9, 'z')) s(id, v) ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET v = s.v", 0);
        assertQuery("SELECT id, v FROM " + table, "VALUES (1, 'a')");
        dropTable(table);
    }

    @Test
    public void testNativeMergeSourceDuplicatesFail()
    {
        String table = "dml_merge_dup_source";
        createV2MergeOnReadTable(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a')", 1);
        assertQueryFails("MERGE INTO " + table + " t USING (VALUES (1, 'x'), (1, 'y')) s(id, v) ON t.id = s.id "
                        + "WHEN MATCHED THEN UPDATE SET v = s.v",
                "(?s).*One or more rows in the target table matched multiple source rows.*");
        assertQuery("SELECT id, v FROM " + table, "VALUES (1, 'a')");
        dropTable(table);
    }
}
