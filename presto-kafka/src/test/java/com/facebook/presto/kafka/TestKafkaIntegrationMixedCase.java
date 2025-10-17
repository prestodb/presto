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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.kafka.util.EmbeddedKafka.createEmbeddedKafka;
import static com.facebook.presto.kafka.util.TestUtils.installKafkaPlugin;
import static com.facebook.presto.kafka.util.TestUtils.loadTpchTopic;
import static com.facebook.presto.kafka.util.TestUtils.loadTpchTopicDescription;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
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
        embeddedKafka = createEmbeddedKafka();
        embeddedKafka.start();

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = new DistributedQueryRunner(createSession(), 2);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            embeddedKafka.createTopics("tpch.orders");

            com.facebook.airlift.json.JsonCodec<com.facebook.presto.kafka.KafkaTopicDescription> codec =
                    new com.facebook.presto.kafka.util.CodecSupplier<>(
                            com.facebook.presto.kafka.KafkaTopicDescription.class,
                            queryRunner.getCoordinator().getMetadata())
                            .get();

            Map.Entry<SchemaTableName, KafkaTopicDescription> lowerEntry =
                    loadTpchTopicDescription(codec, "tpch.orders", new SchemaTableName("tpch", "orders"));

            KafkaTopicDescription upperDesc = new KafkaTopicDescription(
                    "ORDERS",
                    Optional.of("tpch"),
                    lowerEntry.getValue().getTopicName(),
                    lowerEntry.getValue().getKey(),
                    lowerEntry.getValue().getMessage());

            Map<SchemaTableName, KafkaTopicDescription> allTopics =
                    ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                            .put(lowerEntry)
                            .put(new SchemaTableName("tpch", "ORDERS"), upperDesc)
                            .build();

            installKafkaPlugin(
                    embeddedKafka,
                    queryRunner,
                    allTopics,
                    ImmutableMap.of("case-sensitive-name-matching", "true"));

            TestingPrestoClient prestoClient = queryRunner.getRandomClient();
            loadTpchTopic(
                    embeddedKafka,
                    prestoClient,
                    "tpch.orders",
                    new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, "orders"));

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, embeddedKafka);
            throw e;
        }
    }

    private Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("kafka")
                .setSchema("tpch")
                .build();
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
        assertQuerySucceeds(session, "SELECT count(*) FROM ORDERS");
        assertQuerySucceeds(session, "SELECT count(*) FROM tpch.ORDERS");

        // Should fail with wrong case when case-sensitive is enabled
        assertQueryFails(session, "SELECT count(*) FROM Orders", "Table kafka.tpch.Orders does not exist");
        assertQueryFails(session, "SELECT count(*) FROM oRdErS", "Table kafka.tpch.oRdErS does not exist");
        assertQueryFails(session, "SELECT count(*) FROM TPCH.orders", "Schema TPCH does not exist");
    }

    @Test
    public void testDescribeTable()
    {
        try {
            // Should work with exact case
            assertQuerySucceeds(session, "DESCRIBE orders");
            assertQuerySucceeds(session, "DESCRIBE tpch.orders");
            assertQuerySucceeds(session, "DESCRIBE ORDERS");
            assertQuerySucceeds(session, "DESCRIBE tpch.ORDERS");

            // Should fail with wrong case when case-sensitive is enabled
            assertQueryFails(session, "DESCRIBE Orders", ".*");
            assertQueryFails(session, "DESCRIBE oRdErS", ".*");
            assertQueryFails(session, "DESCRIBE TPCH.orders", ".*");
        }
        finally {
            // No cleanup needed for read-only DESCRIBE operations
        }
    }

    @Test
    public void testShowTables()
    {
        // Both lowercase and uppercase tables are registered
        assertQuery(session, "SHOW TABLES", "VALUES ('orders'), ('ORDERS')");
        assertQuery(session, "SHOW TABLES FROM tpch", "VALUES ('orders'), ('ORDERS')");

        assertQueryFails(session, "SHOW TABLES FROM TPCH", "line 1:1: Schema 'TPCH' does not exist");
        assertQueryFails(session, "SHOW TABLES FROM Tpch", "line 1:1: Schema 'Tpch' does not exist");
    }

    @Test
    public void testInformationSchema()
    {
        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders'", "VALUES ('orders')");
        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'ORDERS'", "VALUES ('ORDERS')");

        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_name = 'Orders'", "SELECT 'empty' WHERE false");
        assertQuerySucceeds(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch'");
        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'TPCH'", "SELECT 'empty' WHERE false");
        assertQuery(session, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'Tpch'", "SELECT 'empty' WHERE false");
    }

    @Test
    public void testMixedCaseQueries()
    {
        assertQuerySucceeds(session, "SELECT count(*) FROM orders WHERE orderkey > 100");
        assertQuerySucceeds(session, "SELECT o.orderkey FROM orders o LIMIT 1");

        assertQueryFails(session, "SELECT COUNT(*) FROM Orders WHERE OrderKey > 100", "Table kafka.tpch.Orders does not exist");
        assertQueryFails(session, "SELECT * FROM TPCH.Orders", "Schema TPCH does not exist");
    }

    @Test
    public void testJoinsWithCaseSensitivity()
    {
        assertQuerySucceeds(session, "SELECT count(*) FROM orders o1 JOIN orders o2 ON o1.orderkey = o2.orderkey LIMIT 10");

        assertQueryFails(session, "SELECT count(*) FROM Orders o1 JOIN orders o2 ON o1.orderkey = o2.orderkey", "Table kafka.tpch.Orders does not exist");
        assertQueryFails(session, "SELECT count(*) FROM orders o1 JOIN Orders o2 ON o1.orderkey = o2.orderkey", "Table kafka.tpch.Orders does not exist");
    }

    @Test
    public void testSelectAndDescribeForOrdersAndORDERS()
    {
        assertQuerySucceeds(session, "SELECT count(*) FROM tpch.orders");
        assertQuerySucceeds(session, "SELECT count(*) FROM tpch.\"ORDERS\"");

        assertQuerySucceeds(session,
                "SELECT CASE WHEN (SELECT count(*) FROM tpch.orders) = (SELECT count(*) FROM tpch.\"ORDERS\") THEN 1 ELSE 0 END");

        assertQuerySucceeds(session,
                "SELECT CASE WHEN (SELECT min(orderkey) FROM tpch.orders) = (SELECT min(orderkey) FROM tpch.\"ORDERS\") THEN 1 ELSE 0 END");
        assertQuerySucceeds(session,
                "SELECT CASE WHEN (SELECT max(totalprice) FROM tpch.orders) = (SELECT max(totalprice) FROM tpch.\"ORDERS\") THEN 1 ELSE 0 END");

        assertQuerySucceeds(session, "DESCRIBE tpch.orders");
        assertQuerySucceeds(session, "DESCRIBE tpch.\"ORDERS\"");

        assertQuerySucceeds(session,
                "SELECT count(*) FROM tpch.orders o1 JOIN tpch.\"ORDERS\" o2 ON o1.orderkey = o2.orderkey");

        assertQuerySucceeds(session,
                "SELECT orderstatus, count(*) FROM tpch.orders GROUP BY orderstatus");
        assertQuerySucceeds(session,
                "SELECT orderstatus, count(*) FROM tpch.\"ORDERS\" GROUP BY orderstatus");

        assertQuerySucceeds(session,
                "SELECT CASE WHEN " +
                        "(SELECT count(*) FROM (SELECT orderstatus, count(*) FROM tpch.orders GROUP BY orderstatus)) = " +
                        "(SELECT count(*) FROM (SELECT orderstatus, count(*) FROM tpch.\"ORDERS\" GROUP BY orderstatus)) " +
                        "THEN 1 ELSE 0 END");
    }
}
