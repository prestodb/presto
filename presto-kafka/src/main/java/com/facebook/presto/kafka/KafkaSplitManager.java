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
import io.airlift.log.Logger;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.kafka.KafkaHandleResolver.convertLayout;
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
                }
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
