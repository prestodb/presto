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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;

public class PlainTextKafkaProducerFactory
{
    private final Map<String, Object> properties;

    @Inject
    public PlainTextKafkaProducerFactory(KafkaConnectorConfig kafkaConfig)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        Set<HostAddress> nodes = ImmutableSet.copyOf(kafkaConfig.getNodes());
        properties = ImmutableMap.<String, Object>builder()
                .put(BOOTSTRAP_SERVERS_CONFIG, nodes.stream()
                        .map(HostAddress::toString)
                        .collect(joining(",")))
                .put(ACKS_CONFIG, "all")
                .put(LINGER_MS_CONFIG, 5)
                .build();
    }

    /**
     * Creates a KafkaProducer with the properties set in the constructor.
     */
    public KafkaProducer<byte[], byte[]> create()
    {
        return new KafkaProducer<>(properties, new ByteArraySerializer(), new ByteArraySerializer());
    }
}
