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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.kafka.util.TestUtils.findUnusedPort;
import static com.facebook.presto.kafka.util.TestUtils.toProperties;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class EmbeddedKafka
        implements Closeable
{
    private final ZooKeeperEmbedded zookeeper;
    private final int port;
    private final File kafkaDataDir;
    private final KafkaServer kafka;
    private final AdminClient adminClient;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public static EmbeddedKafka createEmbeddedKafka()
            throws Exception
    {
        return new EmbeddedKafka(new ZooKeeperEmbedded(findUnusedPort()), new Properties());
    }

    public static EmbeddedKafka createEmbeddedKafka(Properties overrideProperties)
            throws Exception
    {
        return new EmbeddedKafka(new ZooKeeperEmbedded(findUnusedPort()), overrideProperties);
    }

    EmbeddedKafka(ZooKeeperEmbedded zookeeper, Properties overrideProperties)
            throws IOException
    {
        this.zookeeper = requireNonNull(zookeeper, "zookeeper is null");
        requireNonNull(overrideProperties, "overrideProperties is null");

        this.port = findUnusedPort();
        this.kafkaDataDir = Files.createTempDir();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("broker.id", "0")
                .put("host.name", "localhost")
                .put("listeners", "PLAINTEXT://localhost:" + getPort())
                .put("num.partitions", "2")
                .put("log.flush.interval.messages", "10000")
                .put("log.flush.interval.ms", "1000")
                .put("log.retention.minutes", "60")
                .put("auto.create.topics.enable", "false")
                .put("zookeeper.connection.timeout.ms", "1000000")
                .put("port", Integer.toString(port))
                .put("log.dirs", kafkaDataDir.getAbsolutePath())
                .put("zookeeper.connect", zookeeper.connectString())
                .put("offsets.topic.replication.factor", "1")
                .putAll(Maps.fromProperties(overrideProperties))
                .build();

        KafkaConfig config = new KafkaConfig(toProperties(properties));
        Time time = Time.SYSTEM;
        this.kafka = new KafkaServer(config, time, scala.Option.empty(), false);
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectString());
        adminProps.put("log.level", "DEBUG");
        this.adminClient = AdminClient.create(adminProps);
    }

    public void start()
            throws InterruptedException, IOException
    {
        if (!started.getAndSet(true)) {
            kafka.startup();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (started.get() && !stopped.getAndSet(true)) {
            kafka.shutdown();
            kafka.awaitShutdown();
            zookeeper.stop();
            deleteRecursively(kafkaDataDir.toPath(), ALLOW_INSECURE);
            adminClient.close();
        }
    }

    public void createTopics(String... topics)
    {
        createTopics(2, 1, new Properties(), topics);
    }

    public void createTopics(int partitions, int replication, Properties topicProperties, String... topics)
    {
        checkState(started.get() && !stopped.get(), "not started!");
        try {
            for (String topic : topics) {
                NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
                newTopic.configs(Maps.fromProperties(topicProperties));
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            }
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to create topics", e);
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

    public int getPort()
    {
        return port;
    }

    public String getConnectString()
    {
        return "localhost:" + Integer.toString(port);
    }

    public String getZookeeperConnectString()
    {
        return zookeeper.connectString();
    }
}
