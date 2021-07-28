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
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.facebook.presto.kafka.util.TestUtils.createEmptyTopicDescription;
import static com.facebook.presto.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestKafkaIntegrationPushDown
        extends AbstractTestQueryFramework
{
    private static final int MESSAGE_NUM = 1000;
    private static final int TIMESTAMP_TEST_COUNT = 6;
    private static final int TIMESTAMP_TEST_START_INDEX = 2;
    private static final int TIMESTAMP_TEST_END_INDEX = 4;

    private static EmbeddedKafka embeddedKafka;
    private static String topicNamePartition;
    private static String topicNameOffset;
    private static String topicNameCreateTime;
    private static String topicNameLogAppend;

    public TestKafkaIntegrationPushDown()
    {
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createKafkaQueryRunner();
    }

    protected static QueryRunner createKafkaQueryRunner()
            throws Exception
    {
        embeddedKafka = EmbeddedKafka.createEmbeddedKafka();
        topicNamePartition = "test_push_down_partition_" + UUID.randomUUID().toString().replaceAll("-", "_");
        topicNameOffset = "test_push_down_offset_" + UUID.randomUUID().toString().replaceAll("-", "_");
        topicNameCreateTime = "test_push_down_create_time_" + UUID.randomUUID().toString().replaceAll("-", "_");
        topicNameLogAppend = "test_push_down_log_append_" + UUID.randomUUID().toString().replaceAll("-", "_");

        QueryRunner queryRunner = KafkaQueryRunner.builder(embeddedKafka)
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createEmptyTopicDescription(topicNamePartition, new SchemaTableName("default", topicNamePartition)))
                        .put(createEmptyTopicDescription(topicNameOffset, new SchemaTableName("default", topicNameOffset)))
                        .put(createEmptyTopicDescription(topicNameCreateTime, new SchemaTableName("default", topicNameCreateTime)))
                        .put(createEmptyTopicDescription(topicNameLogAppend, new SchemaTableName("default", topicNameLogAppend)))
                        .build())
                .setExtraKafkaProperties(ImmutableMap.<String, String>builder()
                        .put("kafka.messages-per-split", "100")
                        .build())
                .build();
        embeddedKafka.createTopicWithConfig(2, 1, topicNamePartition, false);
        embeddedKafka.createTopicWithConfig(2, 1, topicNameOffset, false);
        embeddedKafka.createTopicWithConfig(1, 1, topicNameCreateTime, false);
        embeddedKafka.createTopicWithConfig(1, 1, topicNameLogAppend, true);
        return queryRunner;
    }

    DistributedQueryRunner getDistributedQueryRunner()
    {
        return (DistributedQueryRunner) getQueryRunner();
    }

    @Test
    public void testPartitionPushDown()
    {
        createMessages(topicNamePartition);
        String sql = format("SELECT count(*) FROM default.%s WHERE _partition_id=1", topicNamePartition);

        assertEventually(() -> {
            ResultWithQueryId<MaterializedResult> queryResult = getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
            assertEquals(getQueryInfo(getDistributedQueryRunner(), queryResult).getQueryStats().getProcessedInputPositions(), MESSAGE_NUM / 2);
        });
    }

    @Test
    public void testOffsetPushDown()
    {
        createMessages(topicNameOffset);
        assertProcessedInputPossitions(format("SELECT count(*) FROM default.%s WHERE _partition_offset between 2 and 10", topicNameOffset), 18);
        assertProcessedInputPossitions(format("SELECT count(*) FROM default.%s WHERE _partition_offset > 2 and _partition_offset < 10", topicNameOffset), 14);
        assertProcessedInputPossitions(format("SELECT count(*) FROM default.%s WHERE _partition_offset = 3", topicNameOffset), 2);
    }

    private void assertProcessedInputPossitions(String sql, long expectedProcessedInputPositions)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        assertEventually(() -> {
            ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(getSession(), sql);
            assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions(), expectedProcessedInputPositions);
        });
    }

    @Test
    public void testTimestampCreateTimeModePushDown()
            throws Exception
    {
        RecordMessage recordMessage = createTimestampTestMessages(topicNameCreateTime);
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        // ">= startTime" insure including index 2, "< endTime"  insure excluding index 4;
        // TODO support the pushdown of timestamp to unix time conversion
        // "SELECT count(*) FROM default.%s WHERE _timestamp >= timestamp '%s' and _timestamp < timestamp '%s'"
        String sql = format(
                "SELECT count(*) FROM default.%s WHERE _timestamp >= %d AND _timestamp < %d",
                topicNameCreateTime,
                recordMessage.startTime,
                recordMessage.endTime);

        // timestamp_upper_bound_force_push_down_enabled default as false.
        assertEventually(() -> {
            ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(getSession(), sql);
            assertThat(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions())
                    .isEqualTo(998);
        });

        // timestamp_upper_bound_force_push_down_enabled set as true.
        assertEventually(() -> {
            // timestamp_upper_bound_force_push_down_enabled set as true.
            Session sessionWithUpperBoundPushDownEnabled = Session.builder(getSession())
                    .setSystemProperty("kafka.timestamp_upper_bound_force_push_down_enabled", "true")
                    .build();

            ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(sessionWithUpperBoundPushDownEnabled, sql);
            assertThat(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions())
                    .isEqualTo(2);
        });
    }

    @Test
    public void testTimestampLogAppendModePushDown()
            throws Exception
    {
        RecordMessage recordMessage = createTimestampTestMessages(topicNameLogAppend);
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        // ">= startTime" insure including index 2, "< endTime"  insure excluding index 4;
        // TODO support the pushdown of timestamp to unix time conversion
        // "SELECT count(*) FROM default.%s WHERE _timestamp >= to_unixtime(timestamp '%s') and _timestamp < to_unixtime(timestamp '%s')",
        String sql = format(
                "SELECT count(*) FROM default.%s WHERE _timestamp >= %d AND _timestamp < %d",
                topicNameLogAppend,
                recordMessage.startTime,
                recordMessage.endTime);

        assertEventually(() -> {
            ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(getSession(), sql);
            assertThat(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputPositions())
                    .isEqualTo(2);
        });
    }

    private static QueryInfo getQueryInfo(DistributedQueryRunner queryRunner, ResultWithQueryId<MaterializedResult> queryResult)
    {
        return queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryResult.getQueryId());
    }

    private RecordMessage createTimestampTestMessages(String topicName)
            throws Exception
    {
        long startTime = 0;
        long endTime = 0;
        for (int messageNum = 0; messageNum < TIMESTAMP_TEST_COUNT; messageNum++) {
            long value = messageNum;
            RecordMetadata recordMetadata = embeddedKafka.sendMessages(Stream.of(new ProducerRecord<>(topicName, null, value)));
            if (messageNum == TIMESTAMP_TEST_START_INDEX) {
                startTime = recordMetadata.timestamp();
            }
            else if (messageNum == TIMESTAMP_TEST_END_INDEX) {
                endTime = recordMetadata.timestamp();
            }

            // Sleep for a while to ensure different timestamps for different messages..
            Thread.sleep(100);
        }
        embeddedKafka.sendMessages(
                LongStream.range(TIMESTAMP_TEST_COUNT, MESSAGE_NUM)
                        .mapToObj(id -> new ProducerRecord<>(topicName, null, id)));
        return new RecordMessage(startTime, endTime);
    }

    private void createMessages(String topicName)
    {
        embeddedKafka.sendMessages(
                IntStream.range(0, MESSAGE_NUM)
                        .mapToObj(id -> new ProducerRecord<>(topicName, null, (long) id)));
    }

    private static class RecordMessage
    {
        private final long startTime;
        private final long endTime;

        public RecordMessage(long startTime, long endTime)
        {
            this.startTime = requireNonNull(startTime, "startTime result is none");
            this.endTime = requireNonNull(endTime, "endTime result is none");
        }
    }
}
