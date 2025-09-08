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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.kafka.KafkaQueryRunner.createKafkaQueryRunner;
import static com.facebook.presto.kafka.util.EmbeddedKafka.createEmbeddedKafka;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        embeddedKafka = createEmbeddedKafka();

        return createKafkaQueryRunner(embeddedKafka, ImmutableList.of(TpchTable.ORDERS),
                ImmutableMap.of("case-sensitive-name-matching", "true"));
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

        assertFalse(getQueryRunner().tableExists(session, "ORDERS"));
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

        // Should fail with wrong case when case-sensitive is enabled
        assertQueryFails(session, "SELECT count(*) FROM ORDERS", "Table kafka.tpch.ORDERS does not exist");
        assertQueryFails(session, "SELECT count(*) FROM Orders", "Table kafka.tpch.Orders does not exist");
        assertQueryFails(session, "SELECT count(*) FROM oRdErS", "Table kafka.tpch.oRdErS does not exist");
        assertQueryFails(session, "SELECT count(*) FROM TPCH.orders", "Schema TPCH does not exist");
        assertQueryFails(session, "SELECT count(*) FROM tpch.ORDERS", "Table kafka.tpch.ORDERS does not exist");
        assertQueryFails(session, "SELECT count(*) FROM TPCH.ORDERS", "Schema TPCH does not exist");
    }

    @Test
    public void testDescribeTable()
    {
        try {
            // Should work with exact case
            assertQuerySucceeds(session, "DESCRIBE orders");
            assertQuerySucceeds(session, "DESCRIBE tpch.orders");

            // Should fail with wrong case when case-sensitive is enabled
            assertQueryFails(session, "DESCRIBE ORDERS", ".*");
            assertQueryFails(session, "DESCRIBE Orders", ".*");
            assertQueryFails(session, "DESCRIBE oRdErS", ".*");
            assertQueryFails(session, "DESCRIBE TPCH.orders", ".*");
            assertQueryFails(session, "DESCRIBE tpch.ORDERS", ".*");
        }
        finally {
            // No cleanup needed for read-only DESCRIBE operations
        }
    }

    @Test
    public void testShowTables()
    {
        // Should only show tables with exact case
        assertQuery(session, "SHOW TABLES", "VALUES ('orders')");
        assertQuery(session, "SHOW TABLES FROM tpch", "VALUES ('orders')");

        // Should fail for wrong case schema when case-sensitive is enabled
        assertQueryFails(session, "SHOW TABLES FROM TPCH", "line 1:1: Schema 'TPCH' does not exist");
        assertQueryFails(session, "SHOW TABLES FROM Tpch", "line 1:1: Schema 'Tpch' does not exist");
    }

    @Test
    public void testInformationSchema()
    {
        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders'", "VALUES ('orders')");

        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'ORDERS'", "SELECT 'empty' WHERE false");
        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'Orders'", "SELECT 'empty' WHERE false");

        assertQuerySucceeds(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch'");
        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'TPCH'", "SELECT 'empty' WHERE false");
        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'Tpch'", "SELECT 'empty' WHERE false");
    }
}
