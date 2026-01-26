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
package com.facebook.presto.cassandra;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.cassandra.CassandraTestingUtils.createKeyspace;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.tests.QueryAssertions.assertContainsEventually;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestCassandraIntergrationMixedCase
        extends AbstractTestQueryFramework
{
    private CassandraServer server;
    private CassandraSession session;
    private static Logger log = Logger.get(TestCassandraIntergrationMixedCase.class);
    private static final String KEYSPACE = "test_connector";

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        this.server = new CassandraServer();
        this.session = server.getSession();
        createKeyspace(session, KEYSPACE);
        return CassandraQueryRunner.createCassandraQueryRunner(server, ImmutableMap.of("case-sensitive-name-matching", "true"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        server.close();
    }

    @Test
    public void testCreateTable()
    {
        Session session = testSessionBuilder()
                .setCatalog("cassandra")
                .setSchema(KEYSPACE)
                .build();
        try {
            getQueryRunner().execute(session, "CREATE TABLE TEST_CREATE(name VARCHAR(50), rollNum int)");

            // Add explicit flush to ensure table is persisted
            try {
                log.info("Flushing table %s.TEST_CREATE after CREATE TABLE", KEYSPACE);
                server.flushTablePublic(KEYSPACE, "TEST_CREATE");
            }
            catch (Exception e) {
                log.warn(e, "Failed to flush table after CREATE TABLE, continuing anyway");
            }

            // Driver 4.x: Wait for table to be visible after creation
            waitForTableExists(session, "TEST_CREATE");
            assertTrue(getQueryRunner().tableExists(session, "TEST_CREATE"));

            getQueryRunner().execute(session, "CREATE TABLE  test_create(name VARCHAR(50), rollNum int)");

            // Add explicit flush to ensure table is persisted
            try {
                log.info("Flushing table %s.test_create after CREATE TABLE", KEYSPACE);
                server.flushTablePublic(KEYSPACE, "test_create");
            }
            catch (Exception e) {
                log.warn(e, "Failed to flush table after CREATE TABLE, continuing anyway");
            }

            // Driver 4.x: Wait for table to be visible after creation
            waitForTableExists(session, "test_create");
            assertTrue(getQueryRunner().tableExists(session, "test_create"));

            assertQueryFails(session, "CREATE TABLE TEST_CREATE (name VARCHAR(50), rollNum int)", "line 1:1: Table 'cassandra.test_connector.TEST_CREATE' already exists");
            assertFalse(getQueryRunner().tableExists(session, "Test"));
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS TEST_CREATE");
            assertFalse(getQueryRunner().tableExists(session, "TEST_CREATE"));

            assertUpdate(session, "DROP TABLE IF EXISTS test_create");
            assertFalse(getQueryRunner().tableExists(session, "test_create"));
        }
    }

    @Test
    public void testCreateTableAs()
    {
        Session session = testSessionBuilder()
                .setCatalog("cassandra")
                .setSchema(KEYSPACE)
                .build();
        try {
            getQueryRunner().execute(session, "CREATE TABLE TEST_CREATEAS AS SELECT * FROM tpch.region");
            assertTrue(getQueryRunner().tableExists(session, "TEST_CREATEAS"));

            getQueryRunner().execute(session, "CREATE TABLE IF NOT EXISTS test_createas AS SELECT * FROM tpch.region");
            assertTrue(getQueryRunner().tableExists(session, "test_createas"));

            getQueryRunner().execute(session, "CREATE TABLE TEST_CREATEAS_Join AS SELECT c.custkey, o.orderkey FROM " +
                        "tpch.customer c INNER JOIN tpch.orders o ON c.custkey = o.custkey WHERE c.mktsegment = 'BUILDING'");
            assertTrue(getQueryRunner().tableExists(session, "TEST_CREATEAS_Join"));

            assertQueryFails("CREATE TABLE test_connector.TEST_CREATEAS_FAIL_Join AS SELECT c.custkey, o.orderkey FROM " +
                        "tpch.customer c INNER JOIN tpch.ORDERS o ON c.custkey = o.custkey WHERE c.mktsegment = 'BUILDING'", "Table cassandra.tpch.ORDERS does not exist"); //failure scenario since tpch.ORDERS doesn't exist
            assertFalse(getQueryRunner().tableExists(session, "TEST_CREATEAS_FAIL_Join"));

            getQueryRunner().execute(session, "CREATE TABLE Test_CreateAs_Mixed_Join AS SELECT Cus.custkey, Ord.orderkey FROM " +
                        "tpch.customer Cus INNER JOIN tpch.orders Ord ON Cus.custkey = Ord.custkey WHERE Cus.mktsegment = 'BUILDING'");
            assertTrue(getQueryRunner().tableExists(session, "Test_CreateAs_Mixed_Join"));
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS TEST_CREATEAS");
            assertFalse(getQueryRunner().tableExists(session, "TEST_CREATEAS"));

            assertUpdate(session, "DROP TABLE IF EXISTS test_createas");
            assertFalse(getQueryRunner().tableExists(session, "test_createas"));

            assertUpdate(session, "DROP TABLE IF EXISTS TEST_CREATEAS_Join");
            assertFalse(getQueryRunner().tableExists(session, "TEST_CREATEAS_Join"));

            assertUpdate(session, "DROP TABLE IF EXISTS Test_CreateAs_Mixed_Join");
            assertFalse(getQueryRunner().tableExists(session, "Test_CreateAs_Mixed_Join"));
        }
    }

    @Test
    public void testDuplicatedColumNameCreateTable()
    {
        Session session = testSessionBuilder()
                .setCatalog("cassandra")
                .setSchema(KEYSPACE)
                .build();
        try {
            getQueryRunner().execute(session, "CREATE TABLE test (a integer, A integer)");

            // Add explicit flush to ensure table is persisted
            try {
                log.info("Flushing table %s.test after CREATE TABLE", KEYSPACE);
                server.flushTablePublic(KEYSPACE, "test");
            }
            catch (Exception e) {
                log.warn(e, "Failed to flush table after CREATE TABLE, continuing anyway");
            }

            // Driver 4.x: Wait for table to be visible after creation
            waitForTableExists(session, "test");
            assertTrue(getQueryRunner().tableExists(session, "test"));

            getQueryRunner().execute(session, "CREATE TABLE TEST (a integer, A integer)");

            // Add explicit flush to ensure table is persisted
            try {
                log.info("Flushing table %s.TEST after CREATE TABLE", KEYSPACE);
                server.flushTablePublic(KEYSPACE, "TEST");
            }
            catch (Exception e) {
                log.warn(e, "Failed to flush table after CREATE TABLE, continuing anyway");
            }

            // Driver 4.x: Wait for table to be visible after creation
            waitForTableExists(session, "TEST");
            assertTrue(getQueryRunner().tableExists(session, "TEST"));

            assertQueryFails("CREATE TABLE Test (a integer, a integer)", "line 1:31: Column name 'a' specified more than once");
            assertFalse(getQueryRunner().tableExists(session, "Test"));
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS test");
            assertFalse(getQueryRunner().tableExists(session, "test"));

            assertUpdate(session, "DROP TABLE IF EXISTS TEST");
            assertFalse(getQueryRunner().tableExists(session, "TEST"));
        }
    }

    @Test
    public void testSelect()
    {
        Session session = testSessionBuilder()
                .setCatalog("cassandra")
                .setSchema(KEYSPACE)
                .build();
        try {
            getQueryRunner().execute(session, "CREATE TABLE Test_Select  AS SELECT * FROM tpch.region where regionkey=3");
            assertTrue(getQueryRunner().tableExists(session, "Test_Select"));
            assertQuery("SELECT * from test_connector.Test_Select", "VALUES (3, 'EUROPE', 'ly final courts cajole furiously final excuse')");

            getQueryRunner().execute(session, "CREATE TABLE test_select  AS SELECT * FROM tpch.region LIMIT 2");
            assertQuery("SELECT COUNT(*) FROM test_connector.test_select", "VALUES 2");
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS test_select");
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS Test_Select");
        }
    }

    @Test
    public void testShowSchemas()
    {
        try {
            session.execute("CREATE KEYSPACE \"test_keyspace\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE KEYSPACE \"Test_Keyspace\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
            assertContainsEventually(() -> getQueryRunner().execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                    .row("test_keyspace")
                    .row("Test_Keyspace")
                    .build(), new Duration(1, MINUTES));
        }
        finally {
            session.execute("DROP KEYSPACE \"test_keyspace\"");
            session.execute("DROP KEYSPACE \"Test_Keyspace\"");
        }
    }

    @Test
    public void testUnicodeColumns()
    {
        try {
            session.execute("CREATE KEYSPACE keyspace_1 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
            assertContainsEventually(() -> getQueryRunner().execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                    .row("keyspace_1")
                    .build(), new Duration(1, MINUTES));

            session.execute("CREATE TABLE keyspace_1.table_1 (COLUMN_1 bigint PRIMARY KEY,\"Test用户表\" bigint)");
            assertContainsEventually(() -> getQueryRunner().execute("SHOW TABLES FROM cassandra.keyspace_1"), resultBuilder(getSession(), createUnboundedVarcharType())
                    .row("table_1")
                    .build(), new Duration(1, MINUTES));
            assertContains(
                    getQueryRunner().execute("SHOW COLUMNS FROM cassandra.keyspace_1.table_1"),
                    resultBuilder(getSession(),
                            createUnboundedVarcharType(),
                            createUnboundedVarcharType(),
                            createUnboundedVarcharType(),
                            createUnboundedVarcharType(),
                            createUnboundedVarcharType(),
                            createUnboundedVarcharType(),
                            createUnboundedVarcharType())
                            .row("column_1", "bigint", "", "", Long.valueOf(19), null, null)
                            .row("Test用户表", "bigint", "", "", Long.valueOf(19), null, null)
                            .build());
        }
        finally {
            session.execute("DROP KEYSPACE keyspace_1");
        }
    }

    /**
     * Wait for table to become visible after CREATE TABLE operations.
     * Driver 4.x has aggressive metadata caching and schema changes need time to propagate.
     * Uses exponential backoff for more efficient waiting.
     */
    /**
     * Wait for table to become visible after CREATE TABLE operations.
     * Driver 4.x has aggressive metadata caching and schema changes need time to propagate.
     * Uses exponential backoff for more efficient waiting.
     */
    private void waitForTableExists(Session session, String tableName)
    {
        // P2 FIX: Adaptive timing based on CI environment detection
        // CI environments are slower and need more time for metadata propagation
        boolean isCI = System.getenv("CI") != null || System.getenv("GITHUB_ACTIONS") != null;
        int maxAttempts = isCI ? 120 : 90; // Increased from 90 to 120 for CI
        int baseDelayMs = isCI ? 1000 : 500; // Increased from 500ms to 1000ms for CI
        int maxDelayMs = isCI ? 8000 : 5000; // Increased max delay for CI

        log.info("waitForTableExists: table=%s, maxAttempts=%d, isCI=%s", tableName, maxAttempts, isCI);

        boolean foundInCassandraButNotPresto = false;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (getQueryRunner().tableExists(session, tableName)) {
                if (attempt > 1) {
                    log.info("Table '%s' became visible after %d attempts", tableName, attempt);
                }
                return;
            }

            // More frequent metadata refresh - every 2 attempts instead of 3
            if (attempt % 2 == 0) {
                try {
                    // Verify table exists directly through Cassandra session
                    List<String> tableNames = this.session.getCaseSensitiveTableNames(KEYSPACE);
                    boolean foundDirect = tableNames.stream().anyMatch(name -> name.equalsIgnoreCase(tableName));

                    log.info("Direct Cassandra query shows table '%s' exists: %s (attempt %d/%d, available tables: %s)",
                             tableName, foundDirect, attempt, maxAttempts, tableNames);

                    if (foundDirect && !foundInCassandraButNotPresto) {
                        // Table exists in Cassandra but not visible through Presto yet
                        log.info("Table '%s' found in Cassandra but not in driver metadata - forcing targeted metadata refresh",
                                 tableName);
                        foundInCassandraButNotPresto = true;

                        // Use targeted metadata refresh for faster results
                        server.refreshMetadata(KEYSPACE, tableName);
                        this.session.invalidateKeyspaceCache(KEYSPACE);

                        // P2 FIX: Increased wait time from 1s to 2s to allow metadata propagation
                        // The P0 fixes add internal waits, but we add extra time here for safety
                        Thread.sleep(2000);
                        continue;  // Retry immediately
                    }
                    else if (foundDirect) {
                        // Still not visible, refresh metadata again
                        log.info("Table '%s' still not visible in Presto, refreshing metadata again", tableName);
                        server.refreshMetadata(KEYSPACE, tableName);
                        this.session.invalidateKeyspaceCache(KEYSPACE);
                        // P2 FIX: Increased wait time from 1s to 2s
                        Thread.sleep(2000);
                        continue;  // Retry immediately
                    }
                }
                catch (Exception e) {
                    log.warn("Error during direct Cassandra verification: %s", e.getMessage());
                }
            }

            if (attempt < maxAttempts) {
                int delay = Math.min(baseDelayMs * (1 << Math.min(attempt / 10, 3)), maxDelayMs);

                if (attempt % 10 == 0) {
                    log.info("Still waiting for table '%s' visibility (attempt %d/%d, next delay %dms)",
                             tableName, attempt, maxAttempts, delay);
                }
                try {
                    Thread.sleep(delay);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for table visibility", e);
                }
            }
        }

        // If we get here, table is still not visible after all retries
        log.error("ERROR: Table '%s' not visible after %d attempts (waited approximately %d seconds)",
                 tableName, maxAttempts, maxAttempts * baseDelayMs / 1000);
    }
}
