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

import com.facebook.presto.kafka.server.KafkaClusterMetadataHelper;
import com.facebook.presto.kafka.server.KafkaClusterMetadataSupplier;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_CONSUMER_ERROR;
import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static com.facebook.presto.kafka.KafkaHandleResolver.convertLayout;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific implementation of {@link ConnectorSplitManager}.
 */
public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final KafkaConsumerManager consumerManager;
    private final KafkaClusterMetadataSupplier clusterMetadataSupplier;

    @Inject
    public KafkaSplitManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaClusterMetadataSupplier clusterMetadataSupplier,
            KafkaConsumerManager consumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.clusterMetadataSupplier = requireNonNull(clusterMetadataSupplier, "clusterMetadataSupplier is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        KafkaTableHandle kafkaTableHandle = convertLayout(layout).getTable();
        try {
            String topic = kafkaTableHandle.getTopicName();
            KafkaTableLayoutHandle layoutHandle = (KafkaTableLayoutHandle) layout;
            HostAddress node = KafkaClusterMetadataHelper.selectRandom(clusterMetadataSupplier.getNodes(layoutHandle.getTable().getSchemaName()));

            KafkaConsumer<ByteBuffer, ByteBuffer> consumer = consumerManager.createConsumer(Thread.currentThread().getName(), node);
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

            for (PartitionInfo partition : partitions) {
                Node leader = partition.leader();
                if (leader == null) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Leader election in progress for Kafka topic '%s' partition %s", topic, partition.partition()));
                }

                HostAddress partitionLeader = HostAddress.fromParts(leader.host(), leader.port());
                long startTimestamp = layoutHandle.getStartOffsetTimestamp();
                long endTimestamp = layoutHandle.getEndOffsetTimestamp();

                if (startTimestamp > endTimestamp) {
                    throw new IllegalArgumentException(String.format("Invalid Kafka Offset start/end pair: %s - %s", startTimestamp, endTimestamp));
                }

                TopicPartition topicPartition = new TopicPartition(partition.topic(), partition.partition());
                consumer.assign(ImmutableList.of(topicPartition));

                long beginningOffset = (startTimestamp == 0) ?
                        consumer.beginningOffsets(ImmutableList.of(topicPartition)).values().iterator().next() :
                        findOffsetsByTimestamp(consumer, topicPartition, startTimestamp);
                long endOffset = (endTimestamp == 0) ?
                        consumer.endOffsets(ImmutableList.of(topicPartition)).values().iterator().next() :
                        findOffsetsByTimestamp(consumer, topicPartition, endTimestamp);

                KafkaSplit split = new KafkaSplit(
                        connectorId,
                        topic,
                        kafkaTableHandle.getKeyDataFormat(),
                        kafkaTableHandle.getMessageDataFormat(),
                        kafkaTableHandle.getKeyDataSchemaLocation().map(KafkaSplitManager::readSchema),
                        kafkaTableHandle.getMessageDataSchemaLocation().map(KafkaSplitManager::readSchema),
                        partition.partition(),
                        beginningOffset,
                        endOffset,
                        partitionLeader);
                splits.add(split);
            }

            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof PrestoException) {
                throw e;
            }
            throw new PrestoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.getTableName(), kafkaTableHandle.getTopicName()), e);
        }
    }

    private static long findOffsetsByTimestamp(KafkaConsumer<ByteBuffer, ByteBuffer> consumer, TopicPartition topicPartition, long timestamp)
    {
        try {
            Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsets = consumer.offsetsForTimes(ImmutableMap.of(topicPartition, timestamp));
            if (topicPartitionOffsets == null || topicPartitionOffsets.values().size() == 0) {
                return 0;
            }
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsets.values().iterator().next();
            return offsetAndTimestamp.offset();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(KAFKA_CONSUMER_ERROR, String.format("Failed to find offset by timestamp: %d for partition %d", timestamp, topicPartition.partition()), e);
        }
    }

    private static String readSchema(String dataSchemaLocation)
    {
        InputStream inputStream = null;
        try {
            if (isURI(dataSchemaLocation.trim().toLowerCase(ENGLISH))) {
                try {
                    inputStream = new URL(dataSchemaLocation).openStream();
                }
                catch (MalformedURLException e) {
                    // try again before failing
                    inputStream = new FileInputStream(dataSchemaLocation);
                }
            }
            else {
                inputStream = new FileInputStream(dataSchemaLocation);
            }
            return CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Could not parse the Avro schema at: " + dataSchemaLocation, e);
        }
        finally {
            closeQuietly(inputStream);
        }
    }

    private static void closeQuietly(InputStream stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        }
        catch (IOException ignored) {
        }
    }

    private static boolean isURI(String location)
    {
        try {
            URI.create(location);
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }
}
