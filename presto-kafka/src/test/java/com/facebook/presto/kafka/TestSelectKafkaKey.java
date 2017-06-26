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
import com.facebook.presto.kafka.util.TestUtils;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import kafka.producer.KeyedMessage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.UUID;

import static com.facebook.presto.kafka.util.EmbeddedKafka.CloseableProducer;
import static com.facebook.presto.kafka.util.TestUtils.createTopicDescription;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestSelectKafkaKey
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
            int iMax = 100_000;
            for (long i = 0; i < iMax; i++) {
                ImmutableList.Builder<KeyedMessage<Long, Object>> builder = ImmutableList.builder();
                builder.add(new KeyedMessage<Long, Object>(topicName, i, ImmutableMap.of("id", i, "value", "value:"+i)));
                producer.send(builder.build());
            }
        }
    }

    @AfterClass(alwaysRun = true)
    public void stopKafka()
            throws Exception
    {
        embeddedKafka.close();
    }

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
        this.queryRunner = new StandaloneQueryRunner(SESSION);

        ImmutableList.Builder<KafkaTopicFieldDescription> keyFieldBuilder = ImmutableList.builder();
        keyFieldBuilder.add(new KafkaTopicFieldDescription("kafka_key", BigintType.BIGINT, null, null, "LONG", null, false));
        KafkaTopicFieldGroup key = new KafkaTopicFieldGroup("raw",keyFieldBuilder.build());

        ImmutableList.Builder<KafkaTopicFieldDescription> messageFieldBuilder = ImmutableList.builder();
        messageFieldBuilder.add(new KafkaTopicFieldDescription("id", BigintType.BIGINT, "id", null, "LONG", null, false));
        messageFieldBuilder.add(new KafkaTopicFieldDescription("f_value", VarcharType.VARCHAR, "value", null, "VARCHAR", null, false));
        KafkaTopicFieldGroup message = new KafkaTopicFieldGroup("json", messageFieldBuilder.build());

        TestUtils.installKafkaPlugin(embeddedKafka, queryRunner,
                ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createTopicDescription(topicName, new SchemaTableName("default", topicName), key, message))
                        .build());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        queryRunner.close();
    }

    @Test
    public void testSelectKafkaKeyEqual()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("SELECT kafka_key, id, f_value FROM " + topicName + " WHERE kafka_key = 1000");

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT, BigintType.BIGINT, VarcharType.VARCHAR)
                .row(1000L, 1000L, "value:1000")
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testSelectKafkaKeyLessThan()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("SELECT count(kafka_key) FROM " + topicName + " WHERE kafka_key < 1000");

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1000L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testSelectKafkaKeyGreaterThan()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("SELECT count(kafka_key) FROM " + topicName + " WHERE kafka_key >= 1000");

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(99000L)
                .build();

        assertEquals(result, expected);
    }
}
