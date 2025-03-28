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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.kafka.utils.PropertiesUtils.readProperties;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;

public class PlainTextKafkaProducerFactory
        implements KafkaProducerFactory
{
    private final Map<String, Object> map;
    private final Map<String, String> configurationProperties;

    private static final Logger log = Logger.get(PlainTextKafkaProducerFactory.class);

    @Inject
    public PlainTextKafkaProducerFactory(KafkaConnectorConfig kafkaConfig) throws IOException
    {
        this.map = ImmutableMap.<String, Object>builder()
                .put(ACKS_CONFIG, "all")
                .put(LINGER_MS_CONFIG, 5)
                .build();
        configurationProperties = readProperties(kafkaConfig.getResourceConfigFiles());
    }

    @Override
    public Properties configure(List<HostAddress> bootstrapServers)
    {
        Properties properties = new Properties();
        properties.putAll(map);
        properties.putAll(configurationProperties);
        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.stream()
                        .map(HostAddress::toString)
                        .collect(joining(",")));
        log.info("Loading Default Configs for Kafka...");
        return properties;
    }
}
