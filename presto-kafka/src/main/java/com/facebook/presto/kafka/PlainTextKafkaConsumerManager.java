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

import com.facebook.presto.spi.HostAddress;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;

import javax.inject.Inject;

import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * Manages connections to the Kafka nodes. A worker may connect to multiple Kafka nodes depending on partitions
 * it needs to process.
 */
public class PlainTextKafkaConsumerManager
        implements KafkaConsumerManager
{
    private final int maxPartitionFetchBytes;
    private final int maxPollRecords;

    @Inject
    public PlainTextKafkaConsumerManager(KafkaConnectorConfig kafkaConnectorConfig)
    {
        requireNonNull(kafkaConnectorConfig, "kafkaConnectorConfig is null");
        this.maxPartitionFetchBytes = kafkaConnectorConfig.getMaxPartitionFetchBytes();
        this.maxPollRecords = kafkaConnectorConfig.getMaxPollRecords();
    }

    @Override
    public Properties configure(String threadName, HostAddress hostAddress)
    {
        final Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, hostAddress.toString());
        properties.put(GROUP_ID_CONFIG, threadName);
        properties.put(MAX_POLL_RECORDS_CONFIG, Integer.toString(maxPollRecords));
        properties.put(MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        properties.put(CLIENT_ID_CONFIG, String.format("%s-%s", threadName, hostAddress));
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        return properties;
    }
}
