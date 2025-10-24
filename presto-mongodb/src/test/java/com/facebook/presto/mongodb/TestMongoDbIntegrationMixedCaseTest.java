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
package com.facebook.presto.mongodb;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import io.airlift.tpch.TpchTable;
import org.bson.Document;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.mongodb.MongoQueryRunner.createMongoQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestMongoDbIntegrationMixedCaseTest
        extends AbstractTestQueryFramework
{
    private MongoQueryRunner mongoQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMongoQueryRunner(TpchTable.getTables(), ImmutableMap.of("case-sensitive-name-matching", "true"));
    }

    @BeforeClass
    public void setUp()
    {
        mongoQueryRunner = (MongoQueryRunner) getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (mongoQueryRunner != null) {
            mongoQueryRunner.shutdown();
        }
    }

    public void testDescribeTableWithDifferentCaseInSameSchema()
    {
        try {
            getQueryRunner().execute("CREATE TABLE ORDERS AS SELECT * FROM orders");

            assertTrue(getQueryRunner().tableExists(getSession(), "orders"));
            assertTrue(getQueryRunner().tableExists(getSession(), "ORDERS"));

            MaterializedResult actualColumns = computeActual("DESC ORDERS").toTestTypes();

            MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                    .row("orderkey", "bigint", "", "", 19L, null, null)
                    .row("custkey", "bigint", "", "", 19L, null, null)
                    .row("orderstatus", "varchar(1)", "", "", null, null, 1L)
                    .row("totalprice", "double", "", "", 53L, null, null)
                    .row("orderdate", "date", "", "", null, null, null)
                    .row("orderpriority", "varchar(15)", "", "", null, null, 15L)
                    .row("clerk", "varchar(15)", "", "", null, null, 15L)
                    .row("shippriority", "integer", "", "", 10L, null, null)
                    .row("comment", "varchar(79)", "", "", null, null, 79L)
                    .build();
            assertEquals(actualColumns, expectedColumns);
        }
        finally {
            assertUpdate("DROP TABLE tpch.ORDERS");
        }
    }

    @Test
    public void testCreateAndDropTable()
    {
        Session session = testSessionBuilder()
                .setCatalog("mongodb")
                .setSchema("Mixed_Test_Database")
                .build();

        try {
            getQueryRunner().execute(session, "CREATE TABLE TEST_CREATE(name VARCHAR(50), id int)");
            assertTrue(getQueryRunner().tableExists(session, "TEST_CREATE"));
            assertFalse(getQueryRunner().tableExists(session, "test_create"));

            getQueryRunner().execute(session, "CREATE TABLE test_create(name VARCHAR(50), id int)");
            assertTrue(getQueryRunner().tableExists(session, "test_create"));
            assertFalse(getQueryRunner().tableExists(session, "Test_Create"));
        }

        finally {
            assertUpdate(session, "DROP TABLE TEST_CREATE");
            assertFalse(getQueryRunner().tableExists(session, "TEST_CREATE"));

            assertUpdate(session, "DROP TABLE test_create");
            assertFalse(getQueryRunner().tableExists(session, "test_create"));
        }
    }

    @Test
    public void testCreateTableAs()
    {
        Session session = testSessionBuilder()
                .setCatalog("mongodb")
                .setSchema("Mixed_Test_Database")
                .build();

        try {
            getQueryRunner().execute(session, "CREATE TABLE TEST_CTAS AS SELECT * FROM tpch.region");
            assertTrue(getQueryRunner().tableExists(session, "TEST_CTAS"));

            getQueryRunner().execute(session, "CREATE TABLE IF NOT EXISTS test_ctas AS SELECT * FROM tpch.region");
            assertTrue(getQueryRunner().tableExists(session, "test_ctas"));

            getQueryRunner().execute(session, "CREATE TABLE TEST_CTAS_Join AS SELECT c.custkey, o.orderkey FROM " +
                    "tpch.customer c INNER JOIN tpch.orders o ON c.custkey = o.custkey WHERE c.mktsegment = 'BUILDING'");
            assertTrue(getQueryRunner().tableExists(session, "TEST_CTAS_Join"));

            assertQueryFails("CREATE TABLE Mixed_Test_Database.TEST_CTAS_FAIL_Join AS SELECT c.custkey, o.orderkey FROM " +
                    "tpch.customer c INNER JOIN tpch.ORDERS1 o ON c.custkey = o.custkey WHERE c.mktsegment = 'BUILDING'", "Table mongodb.tpch.ORDERS1 does not exist"); //failure scenario since tpch.ORDERS1 doesn't exist
            assertFalse(getQueryRunner().tableExists(session, "TEST_CTAS_FAIL_Join"));

            getQueryRunner().execute(session, "CREATE TABLE Test_CTAS_Mixed_Join AS SELECT Cus.custkey, Ord.orderkey FROM " +
                    "tpch.customer Cus INNER JOIN tpch.orders Ord ON Cus.custkey = Ord.custkey WHERE Cus.mktsegment = 'BUILDING'");
            assertTrue(getQueryRunner().tableExists(session, "Test_CTAS_Mixed_Join"));
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS TEST_CTAS");
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS test_ctas");
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS TEST_CTAS_Join");
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS Test_CTAS_Mixed_Join");
        }
    }

    @Test
    public void testInsert()
    {
        Session session = testSessionBuilder()
                .setCatalog("mongodb")
                .setSchema("Mixed_Test_Database")
                .build();

        try {
            getQueryRunner().execute(session, "CREATE TABLE Test_Insert (x bigint, y varchar(100))");
            getQueryRunner().execute(session, "INSERT INTO Test_Insert VALUES (123, 'test')");
            assertTrue(getQueryRunner().tableExists(session, "Test_Insert"));
            assertQuery("SELECT * FROM Mixed_Test_Database.Test_Insert", "SELECT 123 x, 'test' y");

            getQueryRunner().execute(session, "CREATE TABLE TEST_INSERT (x bigint, Y varchar(100))");
            getQueryRunner().execute(session, "INSERT INTO TEST_INSERT VALUES (1234, 'test1')");
            assertTrue(getQueryRunner().tableExists(session, "TEST_INSERT"));
            assertQuery("SELECT * FROM Mixed_Test_Database.TEST_INSERT", "SELECT 1234 x, 'test1' Y");
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS Test_Insert");
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS TEST_INSERT");
        }
    }

    @Test
    public void testMixedCaseColumns()
    {
        try {
            assertUpdate("CREATE TABLE test (a integer, B integer)");
            assertUpdate("CREATE TABLE TEST (a integer, aA integer)");
            assertUpdate("INSERT INTO TEST VALUES (123, 12)", 1);
            assertTableColumnNames("TEST", "a", "aA");
            assertUpdate("ALTER TABLE TEST ADD COLUMN EMAIL varchar");
            assertUpdate("ALTER TABLE TEST RENAME COLUMN a TO a_New");
            assertTableColumnNames("TEST", "a_New", "aA", "EMAIL");
            assertUpdate("ALTER TABLE TEST DROP COLUMN aA");
            assertTableColumnNames("TEST", "a_New", "EMAIL");
        }
        finally {
            assertUpdate("DROP TABLE test");
            assertUpdate("DROP TABLE TEST");
        }
    }

    @Test
    public void testShowSchemas()
    {
        // Create two MongoDB databases directly, since Presto doesn't support create schema for mongodb
        MongoClient mongoClient = mongoQueryRunner.getMongoClient();
        try {
            mongoClient.getDatabase("TESTDB1").getCollection("dummy").insertOne(new Document("x", 1));
            mongoClient.getDatabase("testdb1").getCollection("dummy").insertOne(new Document("x", 1));

            MaterializedResult result = computeActual("SHOW SCHEMAS");
            List<String> schemaNames = result.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .collect(Collectors.toList());

            assertTrue(schemaNames.contains("TESTDB1"));
            assertTrue(schemaNames.contains("testdb1"));
        }
        finally {
            mongoClient.getDatabase("TESTDB1").drop();
            mongoClient.getDatabase("testdb1").drop();
        }
    }
}
