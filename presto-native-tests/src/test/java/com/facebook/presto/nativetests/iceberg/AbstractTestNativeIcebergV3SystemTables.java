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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Iceberg metadata tables, maintenance procedures, time travel, branches and tags on V3 tables
 * written by native workers.
 */
public abstract class AbstractTestNativeIcebergV3SystemTables
        extends AbstractNativeIcebergV3Test
{
    private static final DateTimeFormatter UTC_MILLIS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    protected AbstractTestNativeIcebergV3SystemTables(String storageFormat)
    {
        super(storageFormat);
    }

    @Test
    public void testSnapshotsTable()
    {
        String table = "sys_snapshots";
        createV3Table(table, "id integer, region varchar", "partitioning = ARRAY['region']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'us'), (2, 'us'), (3, 'eu')", 3);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 'apac')", 1);
        assertUpdate("DELETE FROM " + table + " WHERE region = 'us'", 2);

        assertQuery(format("SELECT operation FROM \"%s$snapshots\"", table), "VALUES 'append', 'append', 'delete'");
        assertQuery(format("SELECT count(*) FROM \"%s$snapshots\" WHERE parent_id IS NULL", table), "VALUES 1");
        assertQuery(format("SELECT summary['total-records'] FROM \"%s$snapshots\" WHERE operation = 'delete'", table), "VALUES '2'");
        dropTable(table);
    }

    @Test
    public void testFilesTable()
    {
        String table = "sys_files";
        createV3Table(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", 4);
        assertUpdate("INSERT INTO " + table + " VALUES (5, 'e')", 1);

        assertQuery(format("SELECT content, sum(record_count) FROM \"%s$files\" GROUP BY content", table), "VALUES (0, BIGINT '5')");
        assertQuery(format("SELECT count(*) FROM \"%s$files\" WHERE file_size_in_bytes <= 0", table), "VALUES 0");
        assertQuery(format("SELECT count(*) FROM \"%s$files\" WHERE upper(file_format) <> '%s'", table, storageFormat()), "VALUES 0");
        assertDataFilesUseFormat(table);
        dropTable(table);
    }

    @Test
    public void testManifestsAndPartitionsTables()
    {
        String table = "sys_manifests";
        createV3Table(table, "id integer, region varchar", "partitioning = ARRAY['region']");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'us'), (2, 'us'), (3, 'eu')", 3);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 'apac')", 1);

        assertTrue(scalarLong(format("SELECT count(*) FROM \"%s$manifests\"", table)) >= 2, "expected one manifest per append");
        assertQuery(format("SELECT sum(added_data_files_count) FROM \"%s$manifests\"", table), "SELECT count(*) FROM \"" + table + "$files\"");
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", table), "VALUES 3");
        assertQuery(format("SELECT sum(row_count) FROM \"%s$partitions\"", table), "VALUES BIGINT '4'");
        dropTable(table);
    }

    @Test
    public void testHistoryRefsPropertiesAndMetadataLog()
    {
        String table = "sys_history";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2)", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (3)", 1);

        assertQuery(format("SELECT count(*), count_if(is_current_ancestor) FROM \"%s$history\"", table), "VALUES (BIGINT '2', BIGINT '2')");
        assertQuery(format("SELECT name, type FROM \"%s$refs\"", table), "VALUES ('main', 'BRANCH')");
        assertQuery(format("SELECT snapshot_id FROM \"%s$refs\" WHERE name = 'main'", table), "VALUES " + currentSnapshotId(table));
        assertQuery(format("SELECT value FROM \"%s$properties\" WHERE key = 'write.format.default'", table), format("VALUES '%s'", storageFormat()));
        assertEquals(formatVersion(table), 3);
        assertTrue(scalarLong(format("SELECT count(*) FROM \"%s$metadata_log_entries\"", table)) >= 3, "expected one metadata log entry per commit");
        dropTable(table);
    }

    @Test
    public void testChangelogTable()
    {
        // With no snapshot range, $changelog covers the commits after the table's first snapshot.
        String table = "sys_changelog";
        createV3Table(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 'c'), (4, 'd'), (5, 'e')", 3);
        assertQuery(format("SELECT operation, count(*) FROM \"%s$changelog\" GROUP BY operation", table), "VALUES ('INSERT', 3)");
        dropTable(table);
    }

    @Test
    public void testTimeTravel()
    {
        String table = "sys_time_travel";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1)", 1);
        long first = currentSnapshotId(table);
        long firstCommitMillis = loadTable(table).currentSnapshot().timestampMillis();
        assertUpdate("INSERT INTO " + table + " VALUES (2), (3)", 2);

        assertQuery("SELECT id FROM " + table + " FOR VERSION AS OF " + first, "VALUES 1");
        assertQuery("SELECT id FROM " + table, "VALUES 1, 2, 3");
        assertQuery(format("SELECT id FROM %s FOR TIMESTAMP AS OF TIMESTAMP '%s UTC'", table, UTC_MILLIS.format(Instant.ofEpochMilli(firstCommitMillis + 1))), "VALUES 1");
        dropTable(table);
    }

    @Test
    public void testBranchesAndTags()
    {
        String table = "sys_branches";
        createV3Table(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')", 2);
        assertQuerySucceeds("ALTER TABLE " + table + " CREATE BRANCH 'audit'");
        assertQuerySucceeds("ALTER TABLE " + table + " CREATE TAG 'v1'");
        assertUpdate("INSERT INTO " + table + " VALUES (3, 'c')", 1);

        assertQuery("SELECT id FROM " + table + " FOR SYSTEM_VERSION AS OF 'audit'", "VALUES 1, 2");
        assertQuery("SELECT id FROM " + table + " FOR SYSTEM_VERSION AS OF 'v1'", "VALUES 1, 2");
        assertQuery("SELECT id FROM " + table + " FOR SYSTEM_VERSION AS OF 'main'", "VALUES 1, 2, 3");
        assertQuery(format("SELECT name, type FROM \"%s$refs\"", table), "VALUES ('main', 'BRANCH'), ('audit', 'BRANCH'), ('v1', 'TAG')");

        assertQuerySucceeds("ALTER TABLE " + table + " DROP BRANCH 'audit'");
        assertQuerySucceeds("ALTER TABLE " + table + " DROP TAG 'v1'");
        assertQuery(format("SELECT name FROM \"%s$refs\"", table), "VALUES 'main'");
        dropTable(table);
    }

    @Test
    public void testRollbackAndSetCurrentSnapshot()
    {
        String table = "sys_rollback";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2), (3)", 3);
        long first = currentSnapshotId(table);
        assertUpdate("INSERT INTO " + table + " VALUES (4)", 1);
        long second = currentSnapshotId(table);

        assertUpdate(format("CALL system.rollback_to_snapshot('%s', '%s', %s)", SCHEMA, table, first));
        assertQuery("SELECT id FROM " + table, "VALUES 1, 2, 3");
        assertUpdate(format("CALL system.set_current_snapshot('%s', '%s', %s)", SCHEMA, table, second));
        assertQuery("SELECT id FROM " + table, "VALUES 1, 2, 3, 4");

        assertQuerySucceeds("ALTER TABLE " + table + " CREATE BRANCH 'first' FOR SYSTEM_VERSION AS OF " + first);
        assertUpdate(format("CALL system.set_current_snapshot(schema => '%s', table_name => '%s', ref => 'first')", SCHEMA, table));
        assertQuery("SELECT id FROM " + table, "VALUES 1, 2, 3");
        dropTable(table);
    }

    @Test
    public void testRollbackToTimestamp()
    {
        String table = "sys_rollback_ts";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2)", 2);
        long insertMillis = loadTable(table).currentSnapshot().timestampMillis();
        assertUpdate("INSERT INTO " + table + " VALUES (3)", 1);
        assertUpdate(format("CALL system.rollback_to_timestamp('%s', '%s', CAST(from_unixtime(%s / 1e3) AS timestamp))", SCHEMA, table, insertMillis + 1));
        assertQuery("SELECT id FROM " + table, "VALUES 1, 2");
        dropTable(table);
    }

    @Test
    public void testFastForward()
    {
        String table = "sys_fast_forward";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2)", 2);
        assertQuerySucceeds("ALTER TABLE " + table + " CREATE BRANCH 'stale'");
        assertUpdate("INSERT INTO " + table + " VALUES (3)", 1);
        assertQuery("SELECT id FROM " + table + " FOR SYSTEM_VERSION AS OF 'stale'", "VALUES 1, 2");

        assertUpdate(format("CALL system.fast_forward('%s', '%s', 'stale', 'main')", SCHEMA, table));
        assertQuery("SELECT id FROM " + table + " FOR SYSTEM_VERSION AS OF 'stale'", "VALUES 1, 2, 3");
        dropTable(table);
    }

    @Test
    public void testExpireSnapshotsAndRemoveOrphanFiles()
    {
        String table = "sys_expire";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (2)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (3)", 1);
        assertEquals(snapshotCount(table), 3L);

        assertUpdate(format("CALL system.expire_snapshots(schema => '%s', table_name => '%s', older_than => localtimestamp, retain_last => 1)", SCHEMA, table));
        assertEquals(snapshotCount(table), 1L);
        assertUpdate(format("CALL system.remove_orphan_files(schema => '%s', table_name => '%s', older_than => localtimestamp)", SCHEMA, table));
        assertQuery("SELECT id FROM " + table, "VALUES 1, 2, 3");
        dropTable(table);
    }

    @Test
    public void testRewriteManifestsAndDataFiles()
    {
        String table = "sys_rewrite";
        createV3Table(table, "id integer");
        for (int i = 0; i < 5; i++) {
            assertUpdate(format("INSERT INTO %s VALUES (%s)", table, i), 1);
        }
        long manifestsBefore = scalarLong(format("SELECT count(*) FROM \"%s$manifests\"", table));
        assertUpdate(format("CALL system.rewrite_manifests('%s', '%s')", SCHEMA, table));
        assertTrue(scalarLong(format("SELECT count(*) FROM \"%s$manifests\"", table)) < manifestsBefore, "rewrite_manifests must consolidate manifests");

        assertTrue(dataFileCount(table) >= 5, "expected one data file per INSERT");
        assertUpdate(format("CALL system.rewrite_data_files('%s', '%s')", SCHEMA, table), 5);
        assertTrue(dataFileCount(table) < 5, "rewrite_data_files must compact the data files");
        assertQuery("SELECT id FROM " + table, "VALUES 0, 1, 2, 3, 4");
        assertEquals(formatVersion(table), 3);
        assertDataFilesUseFormat(table);
        dropTable(table);
    }

    @Test
    public void testSetTableProperty()
    {
        String table = "sys_set_property";
        createV3Table(table, "id integer");
        assertUpdate(format("CALL system.set_table_property('%s', '%s', 'commit.retry.num-retries', '7')", SCHEMA, table));
        assertQuery(format("SELECT value FROM \"%s$properties\" WHERE key = 'commit.retry.num-retries'", table), "VALUES '7'");
        assertEquals(formatVersion(table), 3);
        dropTable(table);
    }
}
