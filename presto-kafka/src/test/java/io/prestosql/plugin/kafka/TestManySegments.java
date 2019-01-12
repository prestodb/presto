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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.kafka.util.EmbeddedKafka;
import io.prestosql.plugin.kafka.util.TestUtils;
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

import java.util.Properties;
import java.util.UUID;

import static io.prestosql.plugin.kafka.util.EmbeddedKafka.CloseableProducer;
import static io.prestosql.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestManySegments
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

        topicName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");

        Properties topicProperties = new Properties();
        topicProperties.setProperty("segment.bytes", "1048576");

        embeddedKafka.createTopics(1, 1, topicProperties, topicName);

        try (CloseableProducer<Long, Object> producer = embeddedKafka.createProducer()) {
            int jMax = 10_000;
            int iMax = 100_000 / jMax;
            for (long i = 0; i < iMax; i++) {
                ImmutableList.Builder<KeyedMessage<Long, Object>> builder = ImmutableList.builder();
                for (long j = 0; j < jMax; j++) {
                    builder.add(new KeyedMessage<Long, Object>(topicName, i, ImmutableMap.of("id", Long.toString(i * iMax + j), "value", UUID.randomUUID().toString())));
                }
                producer.send(builder.build());
            }
        }
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

    @Test
    public void testManySegments()
    {
        MaterializedResult result = queryRunner.execute("SELECT count(_message) from " + topicName);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(100000L)
                .build();

        assertEquals(result, expected);
    }
}
