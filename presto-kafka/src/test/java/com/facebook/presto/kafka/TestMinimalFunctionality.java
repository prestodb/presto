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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.kafka.util.TestUtils;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static com.facebook.presto.kafka.util.TestUtils.createEmptyTopicDescription;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("kafka")
            .setSchema("default")
            .build();

    private EmbeddedKafka embeddedKafka;
    private String topicName;
    private StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void startKafka()
            throws Exception
    {
        embeddedKafka = EmbeddedKafka.createEmbeddedKafka();
        embeddedKafka.start();
    }

    @AfterClass(alwaysRun = true)
    public void stopKafka()
            throws Exception
    {
        embeddedKafka.close();
        embeddedKafka = null;
    }

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
        this.topicName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");

        Properties topicProperties = new Properties();
        embeddedKafka.createTopics(2, 1, topicProperties, topicName);

        this.queryRunner = new StandaloneQueryRunner(SESSION);

        TestUtils.installKafkaPlugin(embeddedKafka, queryRunner,
                ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createEmptyTopicDescription(topicName, new SchemaTableName("default", topicName)))
                        .build());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    private void createMessages(String topicName, int count)
    {
        try (KafkaProducer<Long, Object> producer = embeddedKafka.createProducer()) {
            int jMax = 10;
            int iMax = count / jMax;
            for (long i = 0; i < iMax; i++) {
                for (long j = 0; j < jMax; j++) {
                    producer.send(new ProducerRecord<>(topicName, i, ImmutableMap.of("id", Long.toString(i * iMax + j), "value", UUID.randomUUID().toString())));
                }
            }
        }
    }

    @Test
    public void testTopicExists()
    {
        QualifiedObjectName name = new QualifiedObjectName("kafka", "default", topicName);

        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(session, name);
                    assertTrue(handle.isPresent());
                });
    }

    @Test
    public void testTopicHasData()
    {
        MaterializedResult result = queryRunner.execute("SELECT count(1) from " + topicName);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();

        assertEquals(result, expected);

        int count = 1000;
        createMessages(topicName, count);

        result = queryRunner.execute("SELECT count(1) from " + topicName);

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row((long) count)
                .build();

        assertEquals(result, expected);
    }
}
