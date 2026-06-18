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
public class TestIcebergCreateTag
        extends AbstractTestQueryFramework
{
    public static final String ICEBERG_CATALOG = "iceberg";
    public static final String TEST_SCHEMA = "test_schema_tag";
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
    public void testCreateTagBasic()
    {
        String tableName = "create_tag_basic_table_test";
        createTable(tableName);

        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE TAG 'test_tag'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'test_tag' and type = 'TAG'", "VALUES 1");
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'test_tag'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'test_tag'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateTagFromVersion()
    {
        String tableName = "create_tag_version_table_test";
        createTable(tableName);

        try {
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES (3, 'Charlie')", 1);
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            assertUpdate(session, format("ALTER TABLE %s CREATE TAG 'version_tag' FOR SYSTEM_VERSION AS OF %d", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'version_tag'", "VALUES 3");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'version_tag'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateTagFromTimestamp()
    {
        String tableName = "create_tag_ts_table_test";
        createTable(tableName);

        try {
            ZonedDateTime committedAt = (ZonedDateTime) computeScalar(session, "SELECT committed_at FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            DateTimeFormatter prestoTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX");
            String timestampLiteral = committedAt.format(prestoTimestamp);
            assertUpdate(session, format("ALTER TABLE %s CREATE TAG 'time_tag' FOR SYSTEM_TIME AS OF TIMESTAMP '%s'", tableName, timestampLiteral));
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'time_tag'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'time_tag'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateTagWithRetention()
    {
        String tableName = "create_tag_retention_table_test";
        createTable(tableName);

        try {
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            assertUpdate(session, format("ALTER TABLE %s CREATE TAG 'retention_tag' FOR SYSTEM_VERSION AS OF %d RETAIN 7 DAYS", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'retention_tag'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'retention_tag'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateTagDuplicate()
    {
        String tableName = "create_tag_duplicate_table_test";
        createTable(tableName);

        try {
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE TAG 'duplicate_tag'");
            assertQueryFails(session, "ALTER TABLE " + tableName + " CREATE TAG 'duplicate_tag'", ".*Tag.*already exists.*");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'duplicate_tag'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateTagWithBothVersionAndTime()
    {
        String tableName = "create_tag_both_table_test";
        createTable(tableName);

        try {
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            ZonedDateTime committedAt = (ZonedDateTime) computeScalar(session, "SELECT committed_at FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            DateTimeFormatter prestoTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX");
            String timestampLiteral = committedAt.format(prestoTimestamp);
            assertQueryFails(session, format("ALTER TABLE " + tableName + " CREATE TAG 'both_tag' FOR SYSTEM_VERSION AS OF %d FOR SYSTEM_TIME AS OF TIMESTAMP '%s'",
                    snapshotId, timestampLiteral), ".*mismatched input.*");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateTagIfNotExists()
    {
        String tableName = "create_tag_ne_table_test";
        createTable(tableName);

        try {
            // Create tag first time - should succeed
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE TAG IF NOT EXISTS 'if_not_exists_tag'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'if_not_exists_tag' and type = 'TAG'", "VALUES 1");

            // Create same tag again with IF NOT EXISTS - should succeed (no-op)
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE TAG IF NOT EXISTS 'if_not_exists_tag'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'if_not_exists_tag' and type = 'TAG'", "VALUES 1");

            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'if_not_exists_tag'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'if_not_exists_tag'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateOrReplaceTag()
    {
        String tableName = "create_tag_replace_table_test";
        createTable(tableName);

        try {
            // Create tag first time
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE TAG 'or_replace_tag'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'or_replace_tag' and type = 'TAG'", "VALUES 1");
            long firstSnapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$refs\" where name = 'or_replace_tag'");
            // Insert more data
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES (4, 'David')", 1);
            // Replace tag - should point to new snapshot
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE OR REPLACE TAG 'or_replace_tag'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'or_replace_tag' and type = 'TAG'", "VALUES 1");
            long secondSnapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$refs\" where name = 'or_replace_tag'");
            // Verify snapshot IDs are different
            if (firstSnapshotId == secondSnapshotId) {
                throw new AssertionError("Expected different snapshot IDs after OR REPLACE");
            }
            // Verify tag now has updated data
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'or_replace_tag'", "VALUES 3");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'or_replace_tag'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateOrReplaceTagNonExistent()
    {
        String tableName = "create_tag_cr_ne_table_test";
        createTable(tableName);

        try {
            // OR REPLACE should work even if tag doesn't exist
            assertUpdate(session, "ALTER TABLE " + tableName + " CREATE OR REPLACE TAG 'new_or_replace_tag'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'new_or_replace_tag' and type = 'TAG'", "VALUES 1");
            assertQuery(session, "SELECT count(*) FROM " + tableName + " FOR SYSTEM_VERSION AS OF 'new_or_replace_tag'", "VALUES 2");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'new_or_replace_tag'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateTagWithBothReplaceAndIfNotExists()
    {
        // Cannot specify both OR REPLACE and IF NOT EXISTS
        assertQueryFails(session, "ALTER TABLE test_table_for_tag CREATE OR REPLACE TAG IF NOT EXISTS 'invalid_tag'", ".*Cannot specify both OR REPLACE and IF NOT EXISTS.*");
    }

    @Test
    public void testCreateTagIfNotExistsWithRetention()
    {
        String tableName = "create_tag_ne_retention";
        createTable(tableName);

        try {
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            // Create with retention
            assertUpdate(session, format("ALTER TABLE %s CREATE TAG IF NOT EXISTS 'retention_if_not_exists' FOR SYSTEM_VERSION AS OF %d RETAIN 7 DAYS", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'retention_if_not_exists' and type = 'TAG'", "VALUES 1");
            // Try to create again - should be no-op
            assertUpdate(session, format("ALTER TABLE %s CREATE TAG IF NOT EXISTS 'retention_if_not_exists' FOR SYSTEM_VERSION AS OF %d RETAIN 14 DAYS", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'retention_if_not_exists' and type = 'TAG'", "VALUES 1");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'retention_if_not_exists'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateOrReplaceTagWithRetention()
    {
        String tableName = "create_tag_cr_with_retention";
        createTable(tableName);

        try {
            long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            // Create with retention
            assertUpdate(session, format("ALTER TABLE %s CREATE TAG 'retention_or_replace' FOR SYSTEM_VERSION AS OF %d RETAIN 7 DAYS", tableName, snapshotId));
            // Replace with different retention
            assertUpdate(session, format("ALTER TABLE %s CREATE OR REPLACE TAG 'retention_or_replace' FOR SYSTEM_VERSION AS OF %d RETAIN 14 DAYS", tableName, snapshotId));
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'retention_or_replace' and type = 'TAG'", "VALUES 1");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'retention_or_replace'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateTagIfTableExists()
    {
        String tableName = "create_tag_table_not_exist";
        createTable(tableName);

        try {
            assertUpdate(session, "ALTER TABLE IF EXISTS " + tableName + " CREATE TAG 'if_exists_tag'");
            assertQuery(session, "SELECT count(*) FROM \"" + tableName + "$refs\" where name = 'if_exists_tag' and type = 'TAG'", "VALUES 1");
            assertUpdate(session, "ALTER TABLE " + tableName + " DROP TAG 'if_exists_tag'");

            assertUpdate(session, "ALTER TABLE IF EXISTS " + tableName + " CREATE TAG 'should_not_fail'");
            assertQueryFails(session, "ALTER TABLE non_existent_table CREATE TAG 'should_fail'", "No value present");
        }
        finally {
            dropTable(tableName);
        }
    }
}
