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
import com.google.common.io.CharStreams;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import java.util.List;
import java.util.Map;

import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static com.facebook.presto.kafka.KafkaHandleResolver.convertLayout;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
    private final KafkaConsumerManager consumerManager;
    private final KafkaClusterMetadataSupplier clusterMetadataSupplier;
    private final KafkaAdminManager adminManager;
    private final int messagesPerSplit;

    @Inject
    public KafkaSplitManager(
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaClusterMetadataSupplier clusterMetadataSupplier,
            KafkaConsumerManager consumerManager,
            KafkaAdminManager adminManager)
    {
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");
        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.clusterMetadataSupplier = requireNonNull(clusterMetadataSupplier, "clusterMetadataSupplier is null");
        this.messagesPerSplit = requireNonNull(kafkaConnectorConfig, "kafkaConnectorConfig is null").getMessagesPerSplit();
        this.adminManager = requireNonNull(adminManager, "adminManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        KafkaTableHandle kafkaTableHandle = convertLayout(layout).getTable();
        KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
        AdminClient adminClient = null;
        try {
            String topic = kafkaTableHandle.getTopicName();
            KafkaTableLayoutHandle layoutHandle = (KafkaTableLayoutHandle) layout;
            HostAddress node = KafkaClusterMetadataHelper.selectRandom(clusterMetadataSupplier.getNodes(layoutHandle.getTable().getSchemaName()));
            kafkaConsumer = consumerManager.createConsumer(Thread.currentThread().getName(), node);
            adminClient = adminManager.createAdmin(node);
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
            List<TopicPartition> topicPartitions = partitionInfos.stream()
                    .map(KafkaSplitManager::toTopicPartition)
                    .collect(toImmutableList());

            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

            Map<TopicPartition, Long> partitionBeginOffsets = kafkaConsumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> partitionEndOffsets = kafkaConsumer.endOffsets(topicPartitions);
            KafkaFilterResult kafkaFilterResult = KafkaFilterManager.getKafkaFilterResult(kafkaConsumer, adminClient, session, kafkaTableHandle,
                    partitionInfos, partitionBeginOffsets, partitionEndOffsets);
            partitionInfos = kafkaFilterResult.getPartitionInfos();
            partitionBeginOffsets = kafkaFilterResult.getPartitionBeginOffsets();
            partitionEndOffsets = kafkaFilterResult.getPartitionEndOffsets();

            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = toTopicPartition(partitionInfo);
                HostAddress leader = HostAddress.fromParts(partitionInfo.leader().host(), partitionInfo.leader().port());
                new Range(partitionBeginOffsets.get(topicPartition), partitionEndOffsets.get(topicPartition))
                        .partition(messagesPerSplit).stream()
                        .map(range -> new KafkaSplit(
                                topic,
                                kafkaTableHandle.getKeyDataFormat(),
                                kafkaTableHandle.getMessageDataFormat(),
                                kafkaTableHandle.getKeyDataSchemaLocation().map(KafkaSplitManager::readSchema),
                                kafkaTableHandle.getMessageDataSchemaLocation().map(KafkaSplitManager::readSchema),
                                partitionInfo.partition(),
                                range,
                                leader))
                        .forEach(splits::add);
            }
            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof PrestoException) {
                throw e;
            }
            throw new PrestoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.getTableName(), kafkaTableHandle.getTopicName()), e);
        }
        finally {
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
            if (adminClient != null) {
                adminClient.close();
            }
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

    private static TopicPartition toTopicPartition(PartitionInfo partitionInfo)
    {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }
}
