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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.kafka.util.TestUtils;
import com.facebook.presto.spi.SchemaTableName;
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

import java.util.UUID;

import static com.facebook.presto.kafka.util.TestUtils.createEmptyTopicDescription;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestKafkaTimestampFilter
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("kafka")
            .setSchema("default")
            .build();

    // Three record timestamps, spaced 5 s apart, well in the past so they are stable.
    // T1 < T2 < T3
    private static final long T1 = 1_700_000_000_000L; // 2023-11-14 22:13:20 UTC
    private static final long T2 = T1 + 5_000L;         // T1 + 5 s
    private static final long T3 = T2 + 5_000L;         // T1 + 10 s

    // A timestamp past all records — no Kafka record will have ts >= AFTER_ALL
    private static final long AFTER_ALL = T3 + 60_000L; // T3 + 60 s

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
    public void setUp()
            throws Exception
    {
        topicName = "ts_filter_" + UUID.randomUUID().toString().replaceAll("-", "_");

        // Single partition so offset-to-timestamp mapping is deterministic
        embeddedKafka.createTopics(1, 1, new java.util.Properties(), topicName);

        queryRunner = new StandaloneQueryRunner(SESSION);

        TestUtils.installKafkaPlugin(embeddedKafka, queryRunner,
                ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createEmptyTopicDescription(topicName, new SchemaTableName("default", topicName)))
                        .build());

        produceRecordsWithTimestamps();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    /**
     * Produces three records with explicit Kafka-record timestamps T1, T2, T3.
     * Each record carries a distinct {@code id} value so COUNT(*) assertions are unambiguous.
     */
    private void produceRecordsWithTimestamps()
    {
        try (KafkaProducer<Long, Object> producer = embeddedKafka.createProducer()) {
            // ProducerRecord(topic, partition, timestamp, key, value)
            producer.send(new ProducerRecord<>(topicName, 0, T1, 1L, ImmutableMap.of("id", "1")));
            producer.send(new ProducerRecord<>(topicName, 0, T2, 2L, ImmutableMap.of("id", "2")));
            producer.send(new ProducerRecord<>(topicName, 0, T3, 3L, ImmutableMap.of("id", "3")));
            producer.flush();
        }
    }

    @Test
    public void testLowerBoundTimestampFilterWithGreaterThanOperator()
    {
        // Query: _timestamp > T1  =>  records at T2 and T3 must be returned
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp > " + T1;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testLowerBoundTimestampFilterWithGreaterThanOrEqualToOperator()
    {
        // Query: _timestamp >= T1  =>  records at T1, T2 and T3 must be returned
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp >= " + T1;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(3L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testUpperBoundTimestampFilterReturnsAllRows()
    {
        // AFTER_ALL > T3, so every record qualifies
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp < " + AFTER_ALL;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(3L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testUpperBoundTimestampFilterHavingValueBetweenTwoMessagesWithLessThanOperator()
    {
        // timestamp between T1 and T2 and query has _timestamp < timestamp, so record at T1 is only returned
        long timestamp = T2 - 1_000L;
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp < " + timestamp;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testUpperBoundTimestampFilterHavingValueBetweenTwoMessagesWithLessThanOrEqualToOperator()
    {
        // timestamp between T1 and T2 and query has _timestamp <= timestamp, so record at T1 is only returned
        long timestamp = T2 - 1_000L;
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp <= " + timestamp;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testUpperBoundTimestampFilterBeforeAllRecordsReturnsNoRows()
    {
        // A timestamp smaller than T1 means no record qualifies
        long beforeAll = T1 - 1_000L;
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp < " + beforeAll;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testUpperBoundTimestampFilterWithFirstMessageTimestampUsingLessThanOperator()
    {
        // Query: _timestamp < T1, so no record qualifies
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp < " + T1;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testUpperBoundTimestampFilterWithFirstMessageTimestampUsingLessThanOrEqualToOperator()
    {
        // Query: _timestamp <= T1, so record at T1 is returned
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp <= " + T1;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testUpperBoundTimestampFilterHavingLastMessageTimestampUsingLessThanOperator()
    {
        // Query: _timestamp < T3, so records at T1 and T2 are returned
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp < " + T3;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testUpperBoundTimestampFilterHavingLastMessageTimestampUsingLessThanOrEqualToOperator()
    {
        // Query: _timestamp <= T3, so records at T1, T2 and T3 are returned
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp <= " + T3;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(3L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testRangeTimestampFilterWithExclusiveOperators()
    {
        // Window: (T1, T3) exclusive on both ends  =>  only record at T2 qualifies
        String query = "SELECT count(1) FROM " + topicName
                + " WHERE _timestamp > " + T1 + " AND _timestamp < " + T3;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testRangeTimestampFilterWithInclusiveOperators()
    {
        // Window: [T1, T3] ixclusive on both ends  =>  all 3 records must be returned
        String query = "SELECT count(1) FROM " + topicName
                + " WHERE _timestamp >= " + T1 + " AND _timestamp <= " + T3;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(3L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testRangeTimestampFilterWithLowerBoundInclusiveAndUpperBoundExclusive()
    {
        // Window: [T1, T3)  =>  records at T1 and T2 are returned
        String query = "SELECT count(1) FROM " + topicName
                + " WHERE _timestamp >= " + T1 + " AND _timestamp < " + T3;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testRangeTimestampFilterWithLowerBoundExclusiveAndUpperBoundInclusive()
    {
        // Window: (T1, T3]  =>  records at T2 and T3 are returned
        String query = "SELECT count(1) FROM " + topicName
                + " WHERE _timestamp > " + T1 + " AND _timestamp <= " + T3;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testBothLowerAndUpperBeyondAllRecordsReturnsNoRows()
    {
        String muchAfter = Long.toString(AFTER_ALL + 10_000L);
        String query = "SELECT count(1) FROM " + topicName + " WHERE _timestamp > " + AFTER_ALL + " AND _timestamp < " + muchAfter;
        MaterializedResult result = queryRunner.execute(query);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();

        assertEquals(result, expected);
    }
}
