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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestIcebergCreateBranch
        extends AbstractTestQueryFramework
{
    public static final String ICEBERG_CATALOG = "iceberg";
    public static final String TEST_SCHEMA = "test_schema_branch";
    private Session session;
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(TEST_SCHEMA)
                .build();

        return IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .setSchemaName(TEST_SCHEMA)
                .setCreateTpchTables(false)
                .build().getQueryRunner();
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate(session, format("CREATE SCHEMA IF NOT EXISTS %s", TEST_SCHEMA));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertUpdate(session, format("DROP SCHEMA IF EXISTS %s", TEST_SCHEMA));
    }

    private void createTable(String tableName)
    {
        assertUpdate(session, "CREATE TABLE IF NOT EXISTS " + tableName + " (id BIGINT, name VARCHAR) WITH (format = 'PARQUET')");
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, 'Alice'), (2, 'Bob')", 2);
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds(session, "DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    public void testCreateBranchBasic()
    {
        String tableName = "create_branch_basic_table_test";
        createTable(tableName);

        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'test_branch'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'test_branch' and type = 'BRANCH'", "VALUES 1");
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'test_branch'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'test_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateBranchFromVersion()
    {
        String tableName = "create_branch_version_table_test";
        createTable(tableName);

        try {
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES (3, 'Charlie')", 1);
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            assertUpdate(session, format("ALTER TABLE %s CREATE BRANCH 'version_branch' FOR SYSTEM_VERSION AS OF %d", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'version_branch'", "VALUES 3");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'version_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateBranchFromTimestamp()
    {
        String tableName = "create_branch_ts_table_test";
        createTable(tableName);

        try {
            ZonedDateTime committedAt = (ZonedDateTime) computeScalar(session, "SELECT committed_at FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            DateTimeFormatter prestoTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX");
            String timestampLiteral = committedAt.format(prestoTimestamp);
            assertUpdate(session, format("ALTER TABLE %s CREATE BRANCH 'time_branch' FOR SYSTEM_TIME AS OF TIMESTAMP '%s'", tableName, timestampLiteral));
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'time_branch'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'time_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateBranchWithRetention()
    {
        String tableName = "create_branch_retention_table_test";
        createTable(tableName);

        try {
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            assertUpdate(session, format("ALTER TABLE %s CREATE BRANCH 'retention_branch' FOR SYSTEM_VERSION AS OF %d RETAIN 7 DAYS", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'retention_branch'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'retention_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateBranchWithSnapshotRetention()
    {
        String tableName = "create_branch_snapshot_retention";
        createTable(tableName);

        try {
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            assertUpdate(session, format("ALTER TABLE %s CREATE BRANCH 'full_retention_branch' " +
                    "FOR SYSTEM_VERSION AS OF %d RETAIN 7 DAYS WITH SNAPSHOT RETENTION 2 SNAPSHOTS 2 DAYS", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'full_retention_branch'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'full_retention_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateBranchDuplicate()
    {
        String tableName = "create_branch_duplicate_table_test";
        createTable(tableName);

        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'duplicate_branch'");
            assertQueryFails(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'duplicate_branch'", ".*Branch.*already exists.*");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'duplicate_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateBranchWithBothVersionAndTime()
    {
        String tableName = "create_branch_both_table_test";
        createTable(tableName);

        try {
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            ZonedDateTime committedAt = (ZonedDateTime) computeScalar(session, "SELECT committed_at FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            DateTimeFormatter prestoTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX");
            String timestampLiteral = committedAt.format(prestoTimestamp);
            assertQueryFails(session, format("ALTER TABLE " + tableName + " CREATE BRANCH 'both_branch' FOR SYSTEM_VERSION AS OF %d FOR SYSTEM_TIME AS OF TIMESTAMP '%s'",
                    snapshotId, timestampLiteral), ".*mismatched input.*");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateBranchIfNotExists()
    {
        String tableName = "create_branch_ne_table_test";
        createTable(tableName);

        try {
            // Create branch first time - should succeed
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH IF NOT EXISTS 'if_not_exists_branch'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'if_not_exists_branch' and type = 'BRANCH'", "VALUES 1");

            // Create same branch again with IF NOT EXISTS - should succeed (no-op)
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH IF NOT EXISTS 'if_not_exists_branch'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'if_not_exists_branch' and type = 'BRANCH'", "VALUES 1");

            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'if_not_exists_branch'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'if_not_exists_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateOrReplaceBranch()
    {
        String tableName = "create_branch_replace_table_test";
        createTable(tableName);

        try {
            // Create branch first time
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE BRANCH 'or_replace_branch'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'or_replace_branch' and type = 'BRANCH'", "VALUES 1");
            long firstSnapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$refs\" where name = 'or_replace_branch'");
            // Insert more data
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES (4, 'David')", 1);
            // Replace branch - should point to new snapshot
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE OR REPLACE BRANCH 'or_replace_branch'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'or_replace_branch' and type = 'BRANCH'", "VALUES 1");
            long secondSnapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$refs\" where name = 'or_replace_branch'");
            // Verify snapshot IDs are different
            if (firstSnapshotId == secondSnapshotId) {
                throw new AssertionError("Expected different snapshot IDs after OR REPLACE");
            }
            // Verify branch now has updated data
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'or_replace_branch'", "VALUES 3");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'or_replace_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateOrReplaceBranchNonExistent()
    {
        String tableName = "create_branch_cr_ne_table_test";
        createTable(tableName);

        try {
            // OR REPLACE should work even if branch doesn't exist
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE OR REPLACE BRANCH 'new_or_replace_branch'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'new_or_replace_branch' and type = 'BRANCH'", "VALUES 1");
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'new_or_replace_branch'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'new_or_replace_branch'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateBranchWithBothReplaceAndIfNotExists()
    {
        // Cannot specify both OR REPLACE and IF NOT EXISTS
        assertQueryFails(session, "ALTER TABLE test_table_for_branch CREATE OR REPLACE BRANCH IF NOT EXISTS 'invalid_branch'", ".*Cannot specify both OR REPLACE and IF NOT EXISTS.*");
    }

    @Test
    public void testCreateBranchIfNotExistsWithRetention()
    {
        String tableName = "create_branch_ne_retention";
        createTable(tableName);

        try {
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            // Create with retention
            assertUpdate(session, format("ALTER TABLE %s CREATE BRANCH IF NOT EXISTS 'retention_if_not_exists' FOR SYSTEM_VERSION AS OF %d RETAIN 7 DAYS", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'retention_if_not_exists' and type = 'BRANCH'", "VALUES 1");
            // Try to create again - should be no-op
            assertUpdate(session, format("ALTER TABLE %s CREATE BRANCH IF NOT EXISTS 'retention_if_not_exists' FOR SYSTEM_VERSION AS OF %d RETAIN 14 DAYS", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'retention_if_not_exists' and type = 'BRANCH'", "VALUES 1");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'retention_if_not_exists'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateOrReplaceBranchWithRetention()
    {
        String tableName = "create_branch_cr_with_retention";
        createTable(tableName);

        try {
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            // Create with retention
            assertUpdate(session, format("ALTER TABLE %s CREATE BRANCH 'retention_or_replace' FOR SYSTEM_VERSION AS OF %d RETAIN 7 DAYS", tableName, snapshotId));
            // Replace with different retention
            assertUpdate(session, format("ALTER TABLE %s CREATE OR REPLACE BRANCH 'retention_or_replace' FOR SYSTEM_VERSION AS OF %d RETAIN 14 DAYS", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'retention_or_replace' and type = 'BRANCH'", "VALUES 1");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'retention_or_replace'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateBranchIfTableExists()
    {
        String tableName = "create_branch_table_not_exist";
        createTable(tableName);

        try {
            assertUpdate(session, "ALTER TABLE IF EXISTS " + tableName + " CREATE BRANCH 'if_exists_branch'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'if_exists_branch' and type = 'BRANCH'", "VALUES 1");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP BRANCH 'if_exists_branch'");

            assertUpdate(session, "ALTER TABLE IF EXISTS " + tableName + " CREATE BRANCH 'should_not_fail'");
            assertQueryFails(session, "ALTER TABLE non_existent_table CREATE BRANCH 'should_fail'", "No value present");
        }
        finally {
            dropTable(tableName);
        }
    }
}
