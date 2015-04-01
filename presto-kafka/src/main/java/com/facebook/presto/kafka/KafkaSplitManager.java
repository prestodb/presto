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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Kafka specific implementation of {@link ConnectorSplitManager}.
 */
public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(KafkaSplitManager.class);

    private final String connectorId;
    private final KafkaConnectorConfig kafkaConnectorConfig;
    private final KafkaHandleResolver handleResolver;
    private final KafkaSimpleConsumerManager consumerManager;

    @Inject
    public KafkaSplitManager(@Named("connectorId") String connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaHandleResolver handleResolver,
            KafkaSimpleConsumerManager consumerManager)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.kafkaConnectorConfig = checkNotNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.consumerManager = checkNotNull(consumerManager, "consumerManager is null");
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
    {
        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(tableHandle);

        List<HostAddress> nodes = new ArrayList<>(kafkaConnectorConfig.getNodes());
        Collections.shuffle(nodes);

        SimpleConsumer simpleConsumer = consumerManager.getConsumer(nodes.get(0));

        try {
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(ImmutableList.of(kafkaTableHandle.getTopicName()));
            TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

            ImmutableList.Builder<ConnectorPartition> builder = ImmutableList.builder();

            for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
                for (PartitionMetadata part : metadata.partitionsMetadata()) {
                    log.debug("Adding Partition %s/%s", metadata.topic(), part.partitionId());
                    Broker leader = part.leader();
                    if (leader == null) { // Leader election going on...
                        log.warn("No leader for partition %s/%s found!", metadata.topic(), part.partitionId());
                    }
                    else {
                        builder.add(new KafkaPartition(metadata.topic(),
                                part.partitionId(),
                                HostAddress.fromParts(leader.host(), leader.port()),
                                ImmutableList.copyOf(Lists.transform(part.isr(), KafkaSplitManager::brokerToHostAddress))));
                    }
                }
            }

            return new ConnectorPartitionResult(builder.build(), tupleDomain);
        }
        catch (Exception e) {
            throw new TableNotFoundException(kafkaTableHandle.toSchemaTableName(), e);
        }
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(tableHandle);

        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        for (ConnectorPartition cp : partitions) {
            checkState(cp instanceof KafkaPartition, "Found an unknown partition type: %s", cp.getClass().getSimpleName());
            KafkaPartition partition = (KafkaPartition) cp;

            SimpleConsumer leaderConsumer = consumerManager.getConsumer(partition.getPartitionLeader());
            // Kafka contains a reverse list of "end - start" pairs for the splits

            long[] offsets = findAllOffsets(leaderConsumer, partition);

            for (int i = offsets.length - 1; i > 0; i--) {
                KafkaSplit split = new KafkaSplit(connectorId,
                        partition.getTopicName(),
                        kafkaTableHandle.getKeyDataFormat(),
                        kafkaTableHandle.getMessageDataFormat(),
                        partition.getPartitionIdAsInt(),
                        offsets[i],
                        offsets[i - 1],
                        partition.getPartitionNodes());
                builder.add(split);
            }
        }

        return new FixedSplitSource(connectorId, builder.build());
    }

    private static long[] findAllOffsets(SimpleConsumer consumer, KafkaPartition partition)
    {
        TopicAndPartition topicAndPartition = new TopicAndPartition(partition.getTopicName(), partition.getPartitionIdAsInt());

        // The API implies that this will always return all of the offsets. So it seems a partition can not have
        // more than Integer.MAX_VALUE-1 segments.
        //
        // This also assumes that the lowest value returned will be the first segment available. So if segments have been dropped off, this value
        // should not be 0.
        PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), Integer.MAX_VALUE);
        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
        OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

        if (offsetResponse.hasError()) {
            short errorCode = offsetResponse.errorCode(partition.getTopicName(), partition.getPartitionIdAsInt());
            log.warn("Offset response has error: %d", errorCode);
            throw new PrestoException(KAFKA_SPLIT_ERROR, "could not fetch data from Kafka, error code is '" + errorCode + "'");
        }

        return offsetResponse.offsets(partition.getTopicName(), partition.getPartitionIdAsInt());
    }

    private static HostAddress brokerToHostAddress(Broker broker)
    {
        return HostAddress.fromParts(broker.host(), broker.port());
    }
}
