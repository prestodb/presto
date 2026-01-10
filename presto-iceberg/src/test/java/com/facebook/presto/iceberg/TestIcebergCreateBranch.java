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
        assertUpdate(session, "CREATE TABLE test_table_for_branch (id BIGINT, name VARCHAR) WITH (format = 'PARQUET')");
        assertUpdate(session, "INSERT INTO test_table_for_branch VALUES (1, 'Alice'), (2, 'Bob')", 2);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertUpdate(session, "DROP TABLE IF EXISTS test_table_for_branch");
        assertUpdate(session, format("DROP SCHEMA IF EXISTS %s", TEST_SCHEMA));
    }

    @Test
    public void testCreateBranchBasic()
    {
        assertUpdate(session, "ALTER TABLE test_table_for_branch CREATE BRANCH 'test_branch'");
        assertQuery(session, "SELECT count(*) FROM \"test_table_for_branch$refs\" where name = 'test_branch' and type = 'BRANCH'", "VALUES 1");
        assertQuery(session, "SELECT count(*) FROM test_table_for_branch FOR SYSTEM_VERSION AS OF 'test_branch'", "VALUES 2");
        assertUpdate(session, "ALTER TABLE test_table_for_branch DROP BRANCH 'test_branch'");
    }

    @Test
    public void testCreateBranchFromVersion()
    {
        assertUpdate(session, "INSERT INTO test_table_for_branch VALUES (3, 'Charlie')", 1);
        long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"test_table_for_branch$snapshots\" ORDER BY committed_at DESC LIMIT 1");
        assertUpdate(session, format("ALTER TABLE test_table_for_branch CREATE BRANCH 'version_branch' FOR SYSTEM_VERSION AS OF %d", snapshotId));
        assertQuery(session, "SELECT count(*) FROM test_table_for_branch FOR SYSTEM_VERSION AS OF 'version_branch'", "VALUES 3");
        assertUpdate(session, "ALTER TABLE test_table_for_branch DROP BRANCH 'version_branch'");
    }

    @Test
    public void testCreateBranchFromTimestamp()
    {
        ZonedDateTime committedAt = (ZonedDateTime) computeScalar(session, "SELECT committed_at FROM \"test_table_for_branch$snapshots\" ORDER BY committed_at DESC LIMIT 1");
        DateTimeFormatter prestoTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX");
        String timestampLiteral = committedAt.format(prestoTimestamp);
        assertUpdate(session, format("ALTER TABLE test_table_for_branch CREATE BRANCH 'time_branch' FOR SYSTEM_TIME AS OF TIMESTAMP '%s'", timestampLiteral));
        assertQuery(session, "SELECT count(*) FROM test_table_for_branch FOR SYSTEM_VERSION AS OF 'time_branch'", "VALUES 2");
        assertUpdate(session, "ALTER TABLE test_table_for_branch DROP BRANCH 'time_branch'");
    }

    @Test
    public void testCreateBranchWithRetention()
    {
        long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"test_table_for_branch$snapshots\" ORDER BY committed_at DESC LIMIT 1");
        assertUpdate(session, format("ALTER TABLE test_table_for_branch CREATE BRANCH 'retention_branch' FOR SYSTEM_VERSION AS OF %d RETAIN 7 DAYS", snapshotId));
        assertQuery(session, "SELECT count(*) FROM test_table_for_branch FOR SYSTEM_VERSION AS OF 'retention_branch'", "VALUES 3");
        assertUpdate(session, "ALTER TABLE test_table_for_branch DROP BRANCH 'retention_branch'");
    }

    @Test
    public void testCreateBranchWithSnapshotRetention()
    {
        long snapshotId = (Long) computeScalar(session, "SELECT snapshot_id FROM \"test_table_for_branch$snapshots\" ORDER BY committed_at DESC LIMIT 1");
        assertUpdate(session, format("ALTER TABLE test_table_for_branch CREATE BRANCH 'full_retention_branch' " +
                "FOR SYSTEM_VERSION AS OF %d RETAIN 7 DAYS WITH SNAPSHOT RETENTION 2 SNAPSHOTS 2 DAYS", snapshotId));
        assertQuery(session, "SELECT count(*) FROM test_table_for_branch FOR SYSTEM_VERSION AS OF 'full_retention_branch'", "VALUES 3");
        assertUpdate(session, "ALTER TABLE test_table_for_branch DROP BRANCH 'full_retention_branch'");
    }

    @Test
    public void testCreateBranchDuplicate()
    {
        assertUpdate(session, "ALTER TABLE test_table_for_branch CREATE BRANCH 'duplicate_branch'");
        assertQueryFails(session, "ALTER TABLE test_table_for_branch CREATE BRANCH 'duplicate_branch'", ".*Branch.*already exists.*");
        assertUpdate(session, "ALTER TABLE test_table_for_branch DROP BRANCH 'duplicate_branch'");
    }

    @Test
    public void testCreateBranchWithInvalidSnapshot()
    {
        assertQueryFails(session, "ALTER TABLE test_table_for_branch CREATE BRANCH 'invalid_branch' FOR SYSTEM_VERSION AS OF 999999999", "Cannot set invalid_branch to unknown snapshot: 999999999");
    }
}
