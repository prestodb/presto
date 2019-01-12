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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.kafka.util.EmbeddedKafka;
import io.prestosql.plugin.kafka.util.TestUtils;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.BigintType;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.StandaloneQueryRunner;
import kafka.producer.KeyedMessage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static io.prestosql.plugin.kafka.util.EmbeddedKafka.CloseableProducer;
import static io.prestosql.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.transaction.TransactionBuilder.transaction;
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
        try (CloseableProducer<Long, Object> producer = embeddedKafka.createProducer()) {
            for (long i = 0; i < count; i++) {
                Object message = ImmutableMap.of("id", Long.toString(i), "value", UUID.randomUUID().toString());
                producer.send(new KeyedMessage<>(topicName, i, message));
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
