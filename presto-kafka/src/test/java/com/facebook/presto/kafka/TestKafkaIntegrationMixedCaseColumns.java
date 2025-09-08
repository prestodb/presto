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
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.kafka.util.EmbeddedKafka.createEmbeddedKafka;
import static com.facebook.presto.kafka.util.TestUtils.createEmptyTableDescriptions;
import static com.facebook.presto.kafka.util.TestUtils.createTableDescriptionsWithColumns;
import static com.facebook.presto.kafka.util.TestUtils.installKafkaPlugin;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertTrue;

public class TestKafkaIntegrationMixedCaseColumns
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("kafka")
            .setSchema("tpch")
            .build();

    private EmbeddedKafka embeddedKafka;
    private final String lowerCaseTable = "orders";
    private final String camelCaseTable = "Orders";
    private final String upperCaseTable = "ORDERS";
    private final String testTable = "ordersTable";
    private StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void startKafka()
            throws Exception
    {
        embeddedKafka = createEmbeddedKafka();
        embeddedKafka.start();
    }

    @AfterClass(alwaysRun = true)
    public void stopKafka()
            throws Exception
    {
        if (embeddedKafka != null) {
            embeddedKafka.close();
            embeddedKafka = null;
        }
    }

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
        this.queryRunner = new StandaloneQueryRunner(SESSION);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        // Create Kafka topic
        embeddedKafka.createTopics("tpch.orders");

        installKafkaPlugin(
                embeddedKafka,
                queryRunner,
                ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .putAll(createEmptyTableDescriptions(
                                new SchemaTableName("tpch", lowerCaseTable),
                                new SchemaTableName("tpch", camelCaseTable),
                                new SchemaTableName("tpch", upperCaseTable)))
                        .putAll(createTableDescriptionsWithColumns(
                                ImmutableMap.of(
                                        new SchemaTableName("tpch", testTable),
                                        ImmutableList.of(
                                                new KafkaTopicFieldDescription("orderkey", BIGINT, "orderkey", null, null, null, false),
                                                new KafkaTopicFieldDescription("OrderKey", BIGINT, "orderkey", null, null, null, false),
                                                new KafkaTopicFieldDescription("ORDERKEY", BIGINT, "orderkey", null, null, null, false),
                                                new KafkaTopicFieldDescription("custkey", BIGINT, "custkey", null, null, null, false),
                                                new KafkaTopicFieldDescription("CustKey", BIGINT, "custkey", null, null, null, false),
                                                new KafkaTopicFieldDescription("CUSTKEY", BIGINT, "custkey", null, null, null, false),
                                                new KafkaTopicFieldDescription("orderstatus", VARCHAR, "orderstatus", null, null, null, false)))))
                        .build(),
                ImmutableMap.of("case-sensitive-name-matching", "true"));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }
    @Test
    public void testTableExists()
    {
        QualifiedObjectName lowerCaseObjName = new QualifiedObjectName("kafka", "tpch", lowerCaseTable);
        QualifiedObjectName camelCaseObjName = new QualifiedObjectName("kafka", "tpch", camelCaseTable);
        QualifiedObjectName upperCaseObjName = new QualifiedObjectName("kafka", "tpch", upperCaseTable);
        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> lowerCaseHandle = queryRunner.getServer().getMetadata().getMetadataResolver(session).getTableHandle(lowerCaseObjName);
                    Optional<TableHandle> camelCaseHandle = queryRunner.getServer().getMetadata().getMetadataResolver(session).getTableHandle(camelCaseObjName);
                    Optional<TableHandle> upperCaseHandle = queryRunner.getServer().getMetadata().getMetadataResolver(session).getTableHandle(upperCaseObjName);
                    assertTrue(lowerCaseHandle.isPresent());
                    assertTrue(camelCaseHandle.isPresent());
                    assertTrue(upperCaseHandle.isPresent());
                });
    }

    private void assertTableCount(String tableName, long expectedCount)
    {
        MaterializedResult result = queryRunner.execute("SELECT count(1) FROM " + tableName);
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BIGINT).row(expectedCount).build();
        assertEquals(result, expected);
    }

    @Test
    public void testShowTables()
    {
        MaterializedResult result = queryRunner.execute("SHOW TABLES FROM kafka.tpch");
        assertEquals(result.getRowCount(), 4);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, createUnboundedVarcharType())
                .row("orders")
                .row("Orders")
                .row("ORDERS")
                .row("ordersTable")
                .build();

        assertContains(result, expected);
    }

    @Test
    public void testShowColumns()
    {
        MaterializedResult result = queryRunner.execute("SHOW COLUMNS FROM kafka.tpch." + testTable);

        // Match the actual format with 7 fields per row
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, createUnboundedVarcharType())
                .row("orderkey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("OrderKey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("ORDERKEY", "bigint", "", "", Long.valueOf(19), null, null)
                .row("custkey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("CustKey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("CUSTKEY", "bigint", "", "", Long.valueOf(19), null, null)
                .row("orderstatus", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .build();
        assertContains(result, expected);
    }

    @Test
    public void testSelect()
    {
        MaterializedResult result = queryRunner.execute("SELECT * FROM " + testTable);
        assertEquals(result.getTypes().size(), 7, "Should have 7 columns with different cases");
    }

    @Test
    public void testSelectWithData()
    {
        MaterializedResult result1 = queryRunner.execute("SELECT count(*) FROM " + lowerCaseTable);
        MaterializedResult result2 = queryRunner.execute("SELECT count(*) FROM " + camelCaseTable);
        MaterializedResult result3 = queryRunner.execute("SELECT count(*) FROM " + upperCaseTable);

        assertEquals(result1.getMaterializedRows().get(0).getField(0),
                result2.getMaterializedRows().get(0).getField(0));
        assertEquals(result2.getMaterializedRows().get(0).getField(0),
                result3.getMaterializedRows().get(0).getField(0));
    }
}
