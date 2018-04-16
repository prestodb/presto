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
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Throwables;
import io.airlift.log.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.inject.Inject;

import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Kafka nodes. A worker may connect to multiple Kafka nodes depending on the segments and partitions
 * it needs to process. According to the Kafka source code, a Kafka {@link kafka.javaapi.consumer.SimpleConsumer} is thread-safe.
 */
public class KafkaConsumerManager
{
    private static final Logger log = Logger.get(KafkaConsumerManager.class);
    private static final String messageDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    private final String connectorId;
    private final NodeManager nodeManager;
    private final int connectTimeoutMillis;
    private final int bufferSizeBytes;
    private final String securityProtocol;
    private final boolean autoCommit;
    private final String bootStrapServers;

    @Inject
    public KafkaConsumerManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.connectTimeoutMillis = toIntExact(kafkaConnectorConfig.getKafkaConnectTimeout().toMillis());
        this.bufferSizeBytes = toIntExact(kafkaConnectorConfig.getKafkaBufferSize().toBytes());
        this.securityProtocol = kafkaConnectorConfig.getSecurityProtocol().name();
        this.autoCommit = kafkaConnectorConfig.isAutoCommit();
        this.bootStrapServers = bootstrapServers(kafkaConnectorConfig.getNodes());
    }

    public KafkaConsumer<byte[], byte[]> getConsumer()
    {
        return getConsumer(bootStrapServers);
    }

    public KafkaConsumer<byte[], byte[]> getConsumer(Set<HostAddress> nodes)
    {
        return getConsumer(bootstrapServers(nodes));
    }

    private KafkaConsumer<byte[], byte[]> getConsumer(String bootstrapServers)
    {
        try {
            Thread.currentThread().setContextClassLoader(null);
            Properties prop = new Properties();
            prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            prop.put(ConsumerConfig.GROUP_ID_CONFIG, connectorId + "-presto");
            prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
            prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, messageDeserializer);
            prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, messageDeserializer);
            prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            prop.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_" + connectorId + "_" + ThreadLocalRandom.current().nextInt() + "_" + System.currentTimeMillis());
            prop.put("security.protocol", securityProtocol);
            return new KafkaConsumer<byte[], byte[]>(prop);
        }
        catch (Exception e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private String bootstrapServers(Set<HostAddress> nodes)
    {
        return nodes.stream().map(ha -> new StringJoiner(":").add(ha.getHostText()).add(String.valueOf(ha.getPort())).toString())
                .collect(Collectors.joining(","));
    }
}
