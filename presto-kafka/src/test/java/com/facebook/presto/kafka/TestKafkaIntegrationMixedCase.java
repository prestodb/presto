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
package com.facebook.presto.kafka;

import com.facebook.presto.Session;
import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.kafka.KafkaQueryRunner.createKafkaQueryRunner;
import static com.facebook.presto.kafka.util.EmbeddedKafka.createEmbeddedKafka;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestKafkaIntegrationMixedCase
        extends AbstractTestQueryFramework
{
    private EmbeddedKafka embeddedKafka;
    private Session session;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        List<SchemaTableName> extraTables = ImmutableList.of(
                new SchemaTableName("TPCH", "ORDERS"));
        embeddedKafka = createEmbeddedKafka();

        return createKafkaQueryRunner(embeddedKafka, ImmutableList.of(TpchTable.ORDERS),
                ImmutableMap.of("case-sensitive-name-matching", "true"), extraTables);
    }

    @BeforeClass(alwaysRun = true)
    public final void setUp()
    {
        session = testSessionBuilder()
                .setCatalog("kafka")
                .setSchema("tpch")
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws IOException
    {
        if (embeddedKafka != null) {
            embeddedKafka.close();
        }
    }

    @Test
    public void testTableExists()
    {
        assertTrue(getQueryRunner().tableExists(session, "orders"));
        assertFalse(getQueryRunner().tableExists(session, "Orders"));
        assertFalse(getQueryRunner().tableExists(session, "oRdErS"));

        assertFalse(getQueryRunner().tableExists(session, "nonexistent"));
        assertFalse(getQueryRunner().tableExists(session, "NONEXISTENT"));
    }

    @Test
    public void testSelect()
    {
        // Should work with exact case
        assertQuerySucceeds(session, "SELECT count(*) FROM orders");
        assertQuerySucceeds(session, "SELECT count(*) FROM tpch.orders");
        assertQuerySucceeds(session, "SELECT count(*) FROM TPCH.ORDERS");
        assertQuerySucceeds(session, "SELECT CASE WHEN (SELECT count(*) FROM tpch.orders) = (SELECT count(*) FROM TPCH.\"ORDERS\") THEN 1 ELSE 0 END");
        assertQuerySucceeds(session, "SELECT CASE WHEN (SELECT min(orderkey) FROM tpch.orders) = (SELECT min(ORDERKEY) FROM TPCH.\"ORDERS\") THEN 1 ELSE 0 END");
        assertQuerySucceeds(session, "SELECT CASE WHEN (SELECT max(totalprice) FROM tpch.orders) = (SELECT max(TOTALPRICE) FROM TPCH.\"ORDERS\") THEN 1 ELSE 0 END");

        // Should fail with wrong case when case-sensitive is enabled
        assertQueryFails(session, "SELECT count(*) FROM Orders", "Table kafka.tpch.Orders does not exist");
        assertQueryFails(session, "SELECT count(*) FROM oRdErS", "Table kafka.tpch.oRdErS does not exist");
        assertQueryFails(session, "SELECT count(*) FROM TPCH.orders", "Table kafka.TPCH.orders does not exist");
    }

    @Test
    public void testDescribeTable()
    {
        assertQuerySucceeds(session, "DESCRIBE orders");
        assertQuerySucceeds(session, "DESCRIBE tpch.orders");
        assertQuerySucceeds(session, "DESCRIBE TPCH.ORDERS");

        assertQueryFails(session, "DESCRIBE Orders", "line 1:1: Table 'kafka.tpch.Orders' does not exist");
        assertQueryFails(session, "DESCRIBE oRdErS", "line 1:1: Table 'kafka.tpch.oRdErS' does not exist");
        assertQueryFails(session, "DESCRIBE TPCH.orders", "line 1:1: Table 'kafka.TPCH.orders' does not exist");

            // Validate full column metadata for lowercase
        assertQuery(
                session,
                "SELECT column_name, data_type FROM information_schema.columns " +
                        "WHERE table_catalog = 'kafka' AND table_schema = 'tpch' AND table_name = 'orders' " +
                        "ORDER BY column_name",
                "SELECT * FROM (VALUES " +
                        "('clerk','varchar(15)')," +
                        "('comment','varchar(79)')," +
                        "('custkey','bigint')," +
                        "('orderdate','date')," +
                        "('orderkey','bigint')," +
                        "('orderpriority','varchar(15)')," +
                        "('orderstatus','varchar(1)')," +
                        "('shippriority','integer')," +
                        "('totalprice','double')) AS t(column_name, data_type)");
            // Validate full column metadata for uppercase
        assertQuery(
                session,
                "SELECT column_name, data_type FROM information_schema.columns " +
                        "WHERE table_catalog = 'kafka' AND table_schema = 'TPCH' AND table_name = 'ORDERS' " +
                        "ORDER BY column_name",
                "SELECT * FROM (VALUES " +
                        "('CLERK','varchar(15)')," +
                        "('COMMENT','varchar(79)')," +
                        "('CUSTKEY','bigint')," +
                        "('ORDERDATE','date')," +
                        "('ORDERKEY','bigint')," +
                        "('ORDERPRIORITY','varchar(15)')," +
                        "('ORDERSTATUS','varchar(1)')," +
                        "('OrderStatus','varchar(1)')," +
                        "('SHIPPRIORITY','integer')," +
                        "('TOTALPRICE','double')) AS t(column_name, data_type)");
    }

    @Test
    public void testShowTables()
    {
        // Both lowercase and uppercase tables are registered
        assertQuery(session, "SHOW TABLES FROM tpch", "VALUES ('orders')");
        assertQuery(session, "SHOW TABLES FROM TPCH", "VALUES ('ORDERS')");
    }

    @Test
    public void testInformationSchema()
    {
        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders'", "VALUES ('orders')");

        assertQuerySucceeds(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch'");
        assertQuerySucceeds(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'TPCH'");

        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'Orders'", "SELECT 'empty' WHERE false");
        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'Tpch'", "SELECT 'empty' WHERE false");
    }

    @Test
    public void testMixedCaseQueries()
    {
        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE orderkey > 100");
        assertQuerySucceeds(session, "SELECT o.orderkey FROM orders o LIMIT 1");

        assertQuerySucceeds(session, "SELECT count(*) FROM TPCH.ORDERS WHERE ORDERKEY > 100");
        assertQuerySucceeds(session, "SELECT o.ORDERKEY FROM TPCH.ORDERS o LIMIT 1");

        assertQueryFails(session, "SELECT COUNT(*) FROM Orders WHERE OrderKey > 100", "Table kafka.tpch.Orders does not exist");
        assertQueryFails(session, "SELECT * FROM TPCH.Orders", "Table kafka.TPCH.Orders does not exist");
    }

    @Test
    public void testJoinsWithCaseSensitivity()
    {
        assertQuerySucceeds(session, "SELECT count(*) FROM orders o1 JOIN orders o2 ON o1.orderkey = o2.orderkey LIMIT 10");

        assertQueryFails(session, "SELECT count(*) FROM Orders o1 JOIN orders o2 ON o1.orderkey = o2.orderkey", "Table kafka.tpch.Orders does not exist");
        assertQueryFails(session, "SELECT count(*) FROM orders o1 JOIN Orders o2 ON o1.orderkey = o2.orderkey", "Table kafka.tpch.Orders does not exist");
    }

    @Test
    public void testSchemaCasing()
    {
        assertQuerySucceeds(session, "SHOW TABLES FROM tpch");
        assertQuerySucceeds("SHOW TABLES FROM TPCH");
        assertQueryFails(session, "SHOW TABLES FROM Tpch", "line 1:1: Schema 'Tpch' does not exist");

        assertQuery(session,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch'",
                "VALUES ('orders')");

        assertQuery(
                session,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'TPCH'",
                "VALUES ('ORDERS')");

        assertQuery(
                session,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'Tpch'",
                "SELECT 'empty' WHERE false");
    }

    @Test
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM tpch.orders");

        MaterializedResult expected = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), createUnboundedVarcharType())
                .row("orderkey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("custkey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("orderstatus", "varchar(1)", "", "", null, null, Long.valueOf(1))
                .row("totalprice", "double", "", "", Long.valueOf(53), null, null)
                .row("orderdate", "date", "", "", null, null, null)
                .row("orderpriority", "varchar(15)", "", "", null, null, Long.valueOf(15))
                .row("clerk", "varchar(15)", "", "", null, null, Long.valueOf(15))
                .row("shippriority", "integer", "", "", Long.valueOf(10), null, null)
                .row("comment", "varchar(79)", "", "", null, null, Long.valueOf(79))
                .build();
        assertContains(actual, expected);
    }
}
