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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestClickhouseIntegrationMixedCase
        extends AbstractTestQueryFramework
{
    private TestingClickHouseServer clickhouseServer;
    private Session session;

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        this.clickhouseServer = new TestingClickHouseServer();
        return createClickHouseQueryRunner(clickhouseServer,
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of("case-sensitive-name-matching", "true"),
                TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (clickhouseServer != null) {
            clickhouseServer.close();
        }
    }

    @BeforeClass(alwaysRun = true)
    public final void setUp()
    {
        session = testSessionBuilder()
                .setCatalog("clickhouse")
                .setSchema("default")
                .build();
    }

    @Test
    public void testCreateTable()
    {
        try {
            getQueryRunner().execute(session, "CREATE TABLE TEST_CREATE(name VARCHAR(50), rollNum int)");
            assertTrue(getQueryRunner().tableExists(session, "TEST_CREATE"));

            getQueryRunner().execute(session, "CREATE TABLE  test_create(name VARCHAR(50), rollNum int)");
            assertTrue(getQueryRunner().tableExists(session, "test_create"));

            assertQueryFails(session, "CREATE TABLE TEST_CREATE (name VARCHAR(50), rollNum int)", "line 1:1: Table 'clickhouse.default.TEST_CREATE' already exists");
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
        try {
            getQueryRunner().execute(session, "CREATE TABLE TEST_CREATEAS AS SELECT * FROM tpch.region");
            assertTrue(getQueryRunner().tableExists(session, "TEST_CREATEAS"));

            getQueryRunner().execute(session, "CREATE TABLE IF NOT EXISTS test_createas AS SELECT * FROM tpch.region");
            assertTrue(getQueryRunner().tableExists(session, "test_createas"));

            getQueryRunner().execute(session, "CREATE TABLE TEST_CREATEAS_Join AS SELECT c.custkey, o.orderkey FROM " +
                    "tpch.customer c INNER JOIN tpch.orders o ON c.custkey = o.custkey WHERE c.mktsegment = 'BUILDING'");
            assertTrue(getQueryRunner().tableExists(session, "TEST_CREATEAS_Join"));

            assertQueryFails("CREATE TABLE test_connector.TEST_CREATEAS_FAIL_Join AS SELECT c.custkey, o.orderkey FROM " +
                    "tpch.customer c INNER JOIN tpch.ORDERS1 o ON c.custkey = o.custkey WHERE c.mktsegment = 'BUILDING'", "Table clickhouse.tpch.ORDERS1 does not exist"); //failure scenario since tpch.ORDERS1 doesn't exist
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

            assertUpdate(session, "DROP TABLE IF EXISTS Test_CreateAs_Mixed_Join");
            assertFalse(getQueryRunner().tableExists(session, "Test_CreateAs_Mixed_Join"));
        }
    }

    @Test
    public void testDuplicatedColumNameCreateTable()
    {
        try {
            getQueryRunner().execute(session, "CREATE TABLE test (a integer, A integer)");
            assertTrue(getQueryRunner().tableExists(session, "test"));

            getQueryRunner().execute(session, "CREATE TABLE TEST (a integer, A integer)");
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
        try {
            getQueryRunner().execute(session, "CREATE TABLE Test_Select  AS SELECT * FROM tpch.region where regionkey=3");
            assertTrue(getQueryRunner().tableExists(session, "Test_Select"));
            assertQuery("SELECT * from default.Test_Select", "VALUES (3, 'EUROPE', 'ly final courts cajole furiously final excuse')");

            getQueryRunner().execute(session, "CREATE TABLE test_select  AS SELECT * FROM tpch.region LIMIT 2");
            assertQuery("SELECT COUNT(*) FROM default.test_select", "VALUES 2");
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS TEST_SELECT");
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS Test_Select");
        }
    }

    @Test
    public void testAlterTable()
    {
        try {
            assertUpdate(" CREATE TABLE Test_Alter (x int NOT NULL, y int, a int) WITH (engine = 'MergeTree', order_by = ARRAY['x'])");
            assertTrue(getQueryRunner().tableExists(getSession(), "Test_Alter"));

            assertQueryFails("ALTER TABLE Test_Alter RENAME COLUMN Y to YYY", "line 1:1: Column 'Y' does not exist");
            assertUpdate("ALTER TABLE Test_Alter RENAME COLUMN y to YYY");

            assertQueryFails("ALTER TABLE IF EXISTS Test_Alter DROP COLUMN notExistColumn", ".*Column 'notExistColumn' does not exist.*");
            assertUpdate("ALTER TABLE Test_Alter DROP COLUMN YYY");
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS Test_Alter");
        }
    }

    @Test
    public void testInsert()
    {
        try {
            getQueryRunner().execute(session, "CREATE TABLE TEST_INSERT(name VARCHAR(50), rollNum int)");
            assertTrue(getQueryRunner().tableExists(session, "TEST_INSERT"));

            assertQueryFails("INSERT INTO Test_Insert VALUES (123, 'test')", ".*Table clickhouse.tpch.Test_Insert does not exist.*");
            getQueryRunner().execute(session, "INSERT INTO TEST_INSERT VALUES ('test', 123)");
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS TEST_INSERT");
        }
    }

    @Test
    public void testUnicodedColumns()
    {
        try {
            getQueryRunner().execute(session, "CREATE TABLE Test_Unicoded(name VARCHAR(50), \"Col用户表\" int)");
            assertTrue(getQueryRunner().tableExists(session, "Test_Unicoded"));

            getQueryRunner().execute(session, "INSERT INTO Test_Unicoded VALUES ('test', 123)");

            getQueryRunner().execute(session, "CREATE TABLE \"Test_ÆØÅ\" (name VARCHAR(50), \"Col用户表\" int)");
            assertTrue(getQueryRunner().tableExists(session, "Test_ÆØÅ"));
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS Test_Unicoded");
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS \"Test_ÆØÅ\" ");
        }
    }

    @Test
    public void testColumnCaseSensitivityWithDuplicateColumns()
    {
        try {
            getQueryRunner().execute(session, "CREATE TABLE case_sensitive_test (user_id Int32, Age Int32, Name String, age Int32, city String) ENGINE = MergeTree() ORDER BY user_id");
            getQueryRunner().execute(session, "INSERT INTO case_sensitive_test VALUES (1, 39, 'Alice', 29, 'Bangalore')");
            assertTrue(getQueryRunner().tableExists(session, "case_sensitive_test"));
            // Test SELECT * works with duplicate columns
            assertQuery(session, "SELECT * FROM case_sensitive_test",
                    "VALUES (1, 39, 'Alice', 29, 'Bangalore')");
            // Test selecting specific columns with exact case
            assertQuery(session, "SELECT Age FROM case_sensitive_test", "VALUES 39");
            assertQuery(session, "SELECT age FROM case_sensitive_test", "VALUES 29");
            // Test WHERE clause with exact case should work
            assertQuery(session, "SELECT * FROM case_sensitive_test WHERE Age > 30",
                    "VALUES (1, 39, 'Alice', 29, 'Bangalore')");
            assertQuery(session, "SELECT * FROM case_sensitive_test WHERE age < 30",
                    "VALUES (1, 39, 'Alice', 29, 'Bangalore')");
            // Test selecting both Age and age columns
            assertQuery(session, "SELECT Age, age FROM case_sensitive_test",
                    "VALUES (39, 29)");
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS case_sensitive_test");
        }
    }

    @Test
    public void testColumnCaseSensitivityColumnNameError()
    {
        try {
            getQueryRunner().execute(session, "CREATE TABLE case_sensitive_error_test (user_id Int32, Age Int32, Name String, city String) ENGINE = MergeTree() ORDER BY user_id");
            getQueryRunner().execute(session, "INSERT INTO case_sensitive_error_test VALUES (1, 39, 'Alice', 'Bangalore')");
            // Test WHERE clause with wrong case should fail
            assertQueryFails(session, "SELECT * FROM case_sensitive_error_test WHERE name = 'Alice'",
                    ".*Column 'name' cannot be resolved.*");
            // Test SELECT with wrong case should fail
            assertQueryFails(session, "SELECT NAME FROM case_sensitive_error_test",
                    ".*Column 'NAME' cannot be resolved.*");
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS case_sensitive_error_test");
        }
    }
}
