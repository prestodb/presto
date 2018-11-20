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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
<<<<<<< HEAD
=======
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
>>>>>>> c6134c443d3475349b87b821fada78a04197b74b
import io.airlift.log.Logger;
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
    private static final Logger log = Logger.get(KafkaSplitManager.class);

    private final String connectorId;
    private final KafkaConsumerManager consumerManager;

    @Inject
    public KafkaSplitManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaConsumerManager consumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        KafkaTableHandle kafkaTableHandle = convertLayout(layout).getTable();
<<<<<<< HEAD
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        try (KafkaConsumer<?, ?> kafkaConsumer = consumerManager.getConsumer()) {
            List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(kafkaTableHandle.getTopicName());
            for (PartitionInfo partitionInfo : partitionInfoList) {
                log.debug("Adding Partition %s/%s", partitionInfo.topic(), partitionInfo.partition());
                if (partitionInfo.leader() == null) { // Leader election going on...
                    log.warn("No leader for partition %s/%s found!", partitionInfo.topic(), partitionInfo.partition());
                    continue;
                }
                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                List<TopicPartition> topicPartitions = ImmutableList.of(topicPartition);
                kafkaConsumer.assign(topicPartitions);
                kafkaConsumer.seekToBeginning(topicPartitions);
                long start = kafkaConsumer.position(topicPartition);
                kafkaConsumer.seekToEnd(topicPartitions);
                long end = kafkaConsumer.position(topicPartition);
                if (start != end) {
                    KafkaSplit split = new KafkaSplit(
                            connectorId,
                            partitionInfo.topic(),
                            kafkaTableHandle.getKeyDataFormat(),
                            kafkaTableHandle.getMessageDataFormat(),
                            partitionInfo.partition(),
                            start,
                            end,
                            HostAddress.fromParts(partitionInfo.leader().host(), partitionInfo.leader().port()));
                    splits.add(split);
=======
        try {
            SimpleConsumer simpleConsumer = consumerManager.getConsumer(selectRandom(nodes));

            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(ImmutableList.of(kafkaTableHandle.getTopicName()));
            TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

            for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
                for (PartitionMetadata part : metadata.partitionsMetadata()) {
                    log.debug("Adding Partition %s/%s", metadata.topic(), part.partitionId());

                    Broker leader = part.leader();
                    if (leader == null) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Leader election in progress for Kafka topic '%s' partition %s", metadata.topic(), part.partitionId()));
                    }

                    HostAddress partitionLeader = HostAddress.fromParts(leader.host(), leader.port());

                    SimpleConsumer leaderConsumer = consumerManager.getConsumer(partitionLeader);
                    // Kafka contains a reverse list of "end - start" pairs for the splits

                    long[] offsets = findAllOffsets(leaderConsumer, metadata.topic(), part.partitionId());

                    for (int i = offsets.length - 1; i > 0; i--) {
                        KafkaSplit split = new KafkaSplit(
                                connectorId,
                                metadata.topic(),
                                kafkaTableHandle.getKeyDataFormat(),
                                kafkaTableHandle.getMessageDataFormat(),
                                kafkaTableHandle.getKeyDataSchemaLocation().map(KafkaSplitManager::readSchema),
                                kafkaTableHandle.getMessageDataSchemaLocation().map(KafkaSplitManager::readSchema),
                                part.partitionId(),
                                offsets[i],
                                offsets[i - 1],
                                partitionLeader);
                        splits.add(split);
                    }
                }
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
>>>>>>> c6134c443d3475349b87b821fada78a04197b74b
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
<<<<<<< HEAD
        return new FixedSplitSource(splits.build());
    }
=======
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

    private static long[] findAllOffsets(SimpleConsumer consumer, String topicName, int partitionId)
    {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, partitionId);

        // The API implies that this will always return all of the offsets. So it seems a partition can not have
        // more than Integer.MAX_VALUE-1 segments.
        //
        // This also assumes that the lowest value returned will be the first segment available. So if segments have been dropped off, this value
        // should not be 0.
        PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), Integer.MAX_VALUE);
        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
        OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

        if (offsetResponse.hasError()) {
            short errorCode = offsetResponse.errorCode(topicName, partitionId);
            throw new RuntimeException("could not fetch data from Kafka, error code is '" + errorCode + "'");
        }

        return offsetResponse.offsets(topicName, partitionId);
    }

    private static <T> T selectRandom(Iterable<T> iterable)
    {
        List<T> list = ImmutableList.copyOf(iterable);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
>>>>>>> c6134c443d3475349b87b821fada78a04197b74b
}
