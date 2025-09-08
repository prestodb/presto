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
                ImmutableMap.of("case-sensitive-name-matching", "false"));
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
        assertTrue(getQueryRunner().tableExists(session, "ORDERS"));
        assertTrue(getQueryRunner().tableExists(session, "Orders"));
        assertTrue(getQueryRunner().tableExists(session, "oRdErS"));

        assertFalse(getQueryRunner().tableExists(session, "nonexistent"));
        assertFalse(getQueryRunner().tableExists(session, "NONEXISTENT"));
    }

    @Test
    public void testSelect()
    {
        assertQuerySucceeds(session, "SELECT count(*) FROM orders");
        assertQuerySucceeds(session, "SELECT count(*) FROM ORDERS");
        assertQuerySucceeds(session, "SELECT count(*) FROM Orders");
        assertQuerySucceeds(session, "SELECT count(*) FROM oRdErS");

        assertQuerySucceeds(session, "SELECT count(*) FROM tpch.orders");
        assertQuerySucceeds(session, "SELECT count(*) FROM TPCH.orders");
        assertQuerySucceeds(session, "SELECT count(*) FROM tpch.ORDERS");
        assertQuerySucceeds(session, "SELECT count(*) FROM TPCH.ORDERS");
    }

    @Test
    public void testDescribeTable()
    {
        assertQuerySucceeds(session, "DESCRIBE orders");
        assertQuerySucceeds(session, "DESCRIBE ORDERS");
        assertQuerySucceeds(session, "DESCRIBE Orders");
        assertQuerySucceeds(session, "DESCRIBE oRdErS");

        assertQuerySucceeds(session, "DESCRIBE tpch.orders");
        assertQuerySucceeds(session, "DESCRIBE TPCH.orders");
        assertQuerySucceeds(session, "DESCRIBE tpch.ORDERS");
    }

    @Test
    public void testShowTables()
    {
        assertQuery(session, "SHOW TABLES", "VALUES ('orders')");
        assertQuery(session, "SHOW TABLES FROM tpch", "VALUES ('orders')");
        assertQuery(session, "SHOW TABLES FROM TPCH", "VALUES ('orders')");
    }

    @Test
    public void testShowSchemas()
    {
        assertQuerySucceeds(session, "SHOW SCHEMAS");
        assertQuerySucceeds(session, "SHOW SCHEMAS FROM kafka");
        assertQuerySucceeds(session, "SHOW SCHEMAS FROM KAFKA");
    }

    @Test
    public void testInformationSchema()
    {
        assertQuerySucceeds(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders'");
        assertQuerySucceeds(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'ORDERS'");
        assertQuerySucceeds(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'Orders'");
    }

    @Test
    public void testColumnNameHandling()
    {
        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE orderkey IS NOT NULL");
        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE ORDERKEY IS NOT NULL");
        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE OrderKey IS NOT NULL");

        assertQuerySucceeds(session, "DESCRIBE orders");
    }

    @Test
    public void testKafkaSpecificColumns()
    {
        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE _partition_id IS NOT NULL");
        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE _message IS NOT NULL");
        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE _key IS NOT NULL");

        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE _PARTITION_ID IS NOT NULL");
        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE _MESSAGE IS NOT NULL");
        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE _KEY IS NOT NULL");
    }

    @Test
    public void testMixedCaseTableNames()
    {
        assertTrue(getQueryRunner().tableExists(session, "orders"));
        assertTrue(getQueryRunner().tableExists(session, "Orders"));
        assertTrue(getQueryRunner().tableExists(session, "ORDERS"));
        assertTrue(getQueryRunner().tableExists(session, "oRdErS"));
    }

    @Test
    public void testMultipleSchemas()
    {
        assertQuerySucceeds(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch'");
        assertQuerySucceeds(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'TPCH'");
        assertQuerySucceeds(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'Tpch'");
    }
}
