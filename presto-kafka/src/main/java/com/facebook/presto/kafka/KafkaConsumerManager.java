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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Kafka nodes. A worker may connect to multiple Kafka nodes depending on partitions
 * it needs to process.
 */
public class KafkaConsumerManager
{
    private static final Logger log = Logger.get(KafkaConsumerManager.class);
    public final LoadingCache<KafkaThreadIdentifier, KafkaConsumer> consumerCache;
    private final int maxPartitionFetchBytes;
    private final int maxPollRecords;

    @Inject
    public KafkaConsumerManager(KafkaConnectorConfig kafkaConnectorConfig)
    {
        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.maxPartitionFetchBytes = kafkaConnectorConfig.getMaxPartitionFetchBytes();

        this.consumerCache = CacheBuilder.newBuilder().build(CacheLoader.from(this::createConsumer));
        this.maxPollRecords = kafkaConnectorConfig.getMaxPollRecords();
    }

    @PreDestroy
    public void tearDown()
    {
        for (Map.Entry<KafkaThreadIdentifier, KafkaConsumer> entry : consumerCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
                consumerCache.invalidate(entry.getKey());
            }
            catch (Exception e) {
                log.warn(e, "While closing consumer %s:", entry.getKey());
            }
        }
    }

    public KafkaConsumer getConsumer(KafkaThreadIdentifier consumerId)
    {
        requireNonNull(consumerId, "host is null");
        return consumerCache.getUnchecked(consumerId);
    }

    KafkaConsumer<String, String> createConsumer(KafkaThreadIdentifier consumerId)
    {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                consumerId.hostAddress.toString());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerId.toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteBufferDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteBufferDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(maxPollRecords));
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId.toString());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        Thread.currentThread().setContextClassLoader(null);
        log.debug("Creating KafkaConsumer for thread %s, partitionId %s", consumerId.threadId, consumerId.partitionId);
        return new KafkaConsumer<>(props);
    }
}
