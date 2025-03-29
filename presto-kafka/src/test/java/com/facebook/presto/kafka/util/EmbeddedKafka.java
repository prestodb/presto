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
package com.facebook.presto.kafka.util;

import com.google.common.io.Files;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.presto.kafka.util.TestUtils.findUnusedPort;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.function.Function.identity;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class EmbeddedKafka
        implements Closeable
{
    private final int port;
    private final File kafkaDataDir;
    private final KafkaClusterTestKit kafka;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public static EmbeddedKafka createEmbeddedKafka()
            throws IOException
    {
        return new EmbeddedKafka();
    }

    EmbeddedKafka()
            throws IOException
    {
        this.port = findUnusedPort();
        this.kafkaDataDir = Files.createTempDir();
        TestKitNodes nodes = new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .setCombined(true)
                .build();
        try {
            this.kafka = new KafkaClusterTestKit.Builder(nodes)
                    .setConfigProp("log.dirs", kafkaDataDir.getAbsolutePath())
                    .build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void start()
            throws Exception
    {
        if (!started.getAndSet(true)) {
            kafka.format();
            kafka.startup();
            kafka.waitForActiveController();
            kafka.waitForReadyBrokers();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (started.get() && !stopped.getAndSet(true)) {
            try {
                kafka.close();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            deleteRecursively(kafkaDataDir.toPath(), ALLOW_INSECURE);
        }
    }

    public void createTopics(String... topics)
    {
        createTopics(2, 1, new Properties(), topics);
    }

    public void createTopics(int partitions, int replication, Properties topicProperties, String... topics)
    {
        checkState(started.get() && !stopped.get(), "not started!");

        try (AdminClient client = AdminClient.create(kafka.clientProperties())) {
            client.createTopics(Arrays.stream(topics)
                    .map(topic -> new NewTopic(topic, partitions, (short) replication)
                            .configs(topicProperties.stringPropertyNames()
                                    .stream()
                                    .collect(toImmutableMap(
                                            identity(),
                                            prop -> (String) topicProperties.get(prop)))))
                    .collect(Collectors.toList()));
        }
    }

    public KafkaProducer<Long, Object> createProducer()
    {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, getConnectString());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, JsonEncoder.class.getName());
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.setProperty(ACKS_CONFIG, "1");

        return new KafkaProducer<>(properties);
    }

    public String getConnectString()
    {
        return kafka.bootstrapServers();
    }
}
