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

import com.facebook.presto.kafka.server.file.FileKafkaClusterMetadataSupplier;
import com.facebook.presto.kafka.server.file.FileKafkaClusterMetadataSupplierConfig;
import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.NotPartitionedPartitionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestKafkaSplitGenerationWithOutOfOrderMessages
{
    // Three record timestamps, spaced 5 s apart, well in the past so they are stable.
    private static final long T1 = 1_700_000_000_000L; // 2023-11-14 22:13:20 UTC
    private static final long T2 = T1 + 5_000L;         // T1 + 5 s
    private static final long T3 = T2 + 5_000L;         // T1 + 10 s

    // A timestamp past all records — no Kafka record has ts >= AFTER_ALL
    private static final long AFTER_ALL = T3 + 60_000L;

    private EmbeddedKafka embeddedKafka;
    private String topicName;
    private KafkaSplitManager splitManager;
    private KafkaTableHandle tableHandle;

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
    {
        topicName = "split_offsets_" + UUID.randomUUID().toString().replaceAll("-", "_");

        // Single partition so offset-to-timestamp mapping is deterministic
        embeddedKafka.createTopics(1, 1, new java.util.Properties(), topicName);

        produceRecordsWithTimestamps();

        // Wire up a KafkaSplitManager directly (no full query runner needed)
        FileKafkaClusterMetadataSupplierConfig clusterConfig = new FileKafkaClusterMetadataSupplierConfig();
        clusterConfig.setNodes(embeddedKafka.getConnectString());
        FileKafkaClusterMetadataSupplier clusterMetadataSupplier = new FileKafkaClusterMetadataSupplier(clusterConfig);

        KafkaConnectorConfig connectorConfig = new KafkaConnectorConfig();
        KafkaConsumerManager consumerManager = new PlainTextKafkaConsumerManager(connectorConfig);

        splitManager = new KafkaSplitManager(
                new KafkaConnectorId("kafka"),
                connectorConfig,
                clusterMetadataSupplier,
                consumerManager);

        // Minimal table handle — no schema files needed for offset tests
        tableHandle = new KafkaTableHandle(
                "kafka",
                "default",
                topicName,
                topicName,
                "dummy",        // keyDataFormat
                "dummy",        // messageDataFormat
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        splitManager = null;
        tableHandle = null;
    }

    // -----------------------------------------------------------------------
    // Helper: produce three records with explicit timestamps
    // -----------------------------------------------------------------------

    private void produceRecordsWithTimestamps()
    {
        try (KafkaProducer<Long, Object> producer = embeddedKafka.createProducer()) {
            producer.send(new ProducerRecord<>(topicName, 0, T1, 1L, "msg-1"));
            producer.send(new ProducerRecord<>(topicName, 0, T1 - 1_000L, 2L, "msg-2"));
            producer.send(new ProducerRecord<>(topicName, 0, T2, 3L, "msg-3"));
            producer.send(new ProducerRecord<>(topicName, 0, T1 - 2_000L, 4L, "msg-4"));
            producer.send(new ProducerRecord<>(topicName, 0, T3, 5L, "msg-5"));
            producer.send(new ProducerRecord<>(topicName, 0, T2 - 2_000L, 6L, "msg-6"));
            producer.flush();
        }
    }

    // -----------------------------------------------------------------------
    // Helper: extract splits for a given timestamp window
    // -----------------------------------------------------------------------

    private List<KafkaSplit> getSplitsForWindow(long startTimestamp, long endTimestamp)
            throws ExecutionException, InterruptedException
    {
        KafkaTableLayoutHandle layout = new KafkaTableLayoutHandle(tableHandle, startTimestamp, endTimestamp);
        ConnectorSplitSource source = splitManager.getSplits(null, null, layout, null);

        Builder<KafkaSplit> result = ImmutableList.builder();
        while (!source.isFinished()) {
            List<ConnectorSplit> batch = source.getNextBatch(NotPartitionedPartitionHandle.NOT_PARTITIONED, 100).get().getSplits();
            batch.forEach(s -> result.add((KafkaSplit) s));
        }
        return result.build();
    }

    @Test
    public void testNoTimestampFilterUsesFullPartitionRange()
            throws Exception
    {
        List<KafkaSplit> splits = getSplitsForWindow(0, Long.MAX_VALUE);

        assertEquals(splits.size(), 1, "Expected exactly one split for a single-partition topic");
        KafkaSplit split = splits.get(0);

        assertEquals(split.getStart(), 0L,
                "With no filter, split should start at partition beginning (offset 0)");
        assertEquals(split.getEnd(), 6L,
                "With no filter, split should end at the log end offset (6 records written)");
    }

    @Test
    public void testLowerBoundTimestampFilterSetsCorrectStartOffset()
            throws Exception
    {
        List<KafkaSplit> splits = getSplitsForWindow(T2, Long.MAX_VALUE);

        assertEquals(splits.size(), 1);
        KafkaSplit split = splits.get(0);

        assertEquals(split.getStart(), 2L,
                "startTimestamp=T2 should resolve to offset 2 (third record)");
        assertEquals(split.getEnd(), 6L,
                "endTimestamp=MAX_VALUE should use the log end offset");
    }

    @Test
    public void testUpperBoundTimestampFilterSetsCorrectEndOffset()
            throws Exception
    {
        List<KafkaSplit> splits = getSplitsForWindow(0, T2);

        assertEquals(splits.size(), 1);
        KafkaSplit split = splits.get(0);

        assertEquals(split.getStart(), 0L,
                "startTimestamp=0 should use partition beginning (offset 0)");
        assertEquals(split.getEnd(), 2L,
                "endTimestamp=T2 should resolve to offset 2 (third record)");
    }

    @Test
    public void testRangeTimestampFilterSetsCorrectOffsets()
            throws Exception
    {
        List<KafkaSplit> splits = getSplitsForWindow(T2, T3);

        assertEquals(splits.size(), 1);
        KafkaSplit split = splits.get(0);

        assertEquals(split.getStart(), 2L,
                "startTimestamp=T2 should resolve to offset 2");
        assertEquals(split.getEnd(), 4L,
                "endTimestamp=T3 should resolve to offset 4");
    }

    @Test
    public void testLowerBoundBeyondAllRecordsProducesEmptySplit()
            throws Exception
    {
        List<KafkaSplit> splits = getSplitsForWindow(AFTER_ALL, AFTER_ALL + 10_000L);

        assertEquals(splits.size(), 1);
        KafkaSplit split = splits.get(0);

        assertEquals(split.getEnd(), 6L, "timestamp well beyond timestamp for the published messages implies we reached the end of stream");
        assertEquals(split.getStart(), split.getEnd(),
                "When startTimestamp is past all records, start should equal end (empty split)");
    }

    @Test
    public void testUpperBoundBeforeAllRecordsSetsCorrectOffsets()
            throws Exception
    {
        long beforeAll = T1 - 1_000L;
        List<KafkaSplit> splits = getSplitsForWindow(0, beforeAll);

        assertEquals(splits.size(), 1);
        KafkaSplit split = splits.get(0);

        assertEquals(split.getStart(), 0L,
                "startTimestamp=0 should use partition beginning");
        assertEquals(split.getEnd(), 0L,
                "offsetsForTimes() returns the earliest offset whose timestamp is greater than or equal to the supplied timestamp");
    }

    @Test
    public void testUpperBoundSetsCorrectOffsets()
            throws Exception
    {
        long timestamp = T2 - 1_000L;
        List<KafkaSplit> splits = getSplitsForWindow(0, timestamp);

        assertEquals(splits.size(), 1);
        KafkaSplit split = splits.get(0);

        assertEquals(split.getStart(), 0L,
                "startTimestamp=0 should use partition beginning");
        assertEquals(split.getEnd(), 2L,
                "offsetsForTimes() returns the earliest offset whose timestamp is greater than or equal to the supplied timestamp");
    }

    @Test
    public void testFirstRecordRangeProducesCorrectOffsets()
            throws Exception
    {
        List<KafkaSplit> splits = getSplitsForWindow(T1, T2);

        assertEquals(splits.size(), 1);
        KafkaSplit split = splits.get(0);

        assertEquals(split.getStart(), 0L,
                "startTimestamp=T1 should resolve to offset 0");
        assertEquals(split.getEnd(), 2L,
                "endTimestamp=T2 should resolve to offset 2");
    }

    @Test
    public void testLowerBoundAtFirstRecordReturnsFullRange()
            throws Exception
    {
        List<KafkaSplit> splits = getSplitsForWindow(T1, Long.MAX_VALUE);

        assertEquals(splits.size(), 1);
        KafkaSplit split = splits.get(0);

        assertEquals(split.getStart(), 0L, "startTimestamp=T1 should resolve to offset 0");
        assertEquals(split.getEnd(), 6L, "Long.MAX_VALUE implies we fetch endOffsets");
    }
}
