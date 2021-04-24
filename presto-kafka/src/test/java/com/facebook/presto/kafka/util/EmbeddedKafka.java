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

import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static com.facebook.presto.kafka.util.TestUtils.findUnusedPort;
import static com.facebook.presto.kafka.util.TestUtils.toProperties;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class EmbeddedKafka
        implements Closeable
{
    private static final Logger log = Logger.get(EmbeddedKafka.class);

    private final EmbeddedZookeeper zookeeper;
    private final int port;
    private final File kafkaDataDir;
    private final KafkaServerStartable kafka;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public static EmbeddedKafka createEmbeddedKafka()
            throws IOException
    {
        return new EmbeddedKafka(new EmbeddedZookeeper(), new Properties());
    }

    public static EmbeddedKafka createEmbeddedKafka(Properties overrideProperties)
            throws IOException
    {
        return new EmbeddedKafka(new EmbeddedZookeeper(), overrideProperties);
    }

    EmbeddedKafka(EmbeddedZookeeper zookeeper, Properties overrideProperties)
            throws IOException
    {
        this.zookeeper = requireNonNull(zookeeper, "zookeeper is null");
        requireNonNull(overrideProperties, "overrideProperties is null");

        this.port = findUnusedPort();
        this.kafkaDataDir = Files.createTempDir();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("broker.id", "0")
                .put("host.name", "localhost")
                .put("num.partitions", "2")
                .put("log.flush.interval.messages", "10000")
                .put("log.flush.interval.ms", "1000")
                .put("log.retention.minutes", "60")
                .put("auto.create.topics.enable", "false")
                .put("zookeeper.connection.timeout.ms", "1000000")
                .put("port", Integer.toString(port))
                .put("log.dirs", kafkaDataDir.getAbsolutePath())
                .put("zookeeper.connect", zookeeper.getConnectString())
                .put("offsets.topic.replication.factor", "1")
                .putAll(Maps.fromProperties(overrideProperties))
                .build();

        KafkaConfig config = new KafkaConfig(toProperties(properties));
        this.kafka = new KafkaServerStartable(config);
    }

    public void start()
            throws InterruptedException, IOException
    {
        if (!started.getAndSet(true)) {
            zookeeper.start();
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
            zookeeper.close();
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

        ZkUtils zkUtils = ZkUtils.apply(getZookeeperConnectString(), 30_000, 30_000, false);
        try {
            for (String topic : topics) {
                AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicProperties, RackAwareMode.Disabled$.MODULE$);
            }
        }
        finally {
            zkUtils.close();
        }
    }

    public KafkaProducer<Long, Object> createProducer()
    {
        return createProducer(new HashMap<>());
    }

    public <K, V> KafkaProducer<K, V> createProducer(Map<String, String> extraProducerProperties)
    {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, getConnectString());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, JsonEncoder.class.getName());
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.setProperty(ACKS_CONFIG, "1");

        return new KafkaProducer<>(properties);
    }

    public int getZookeeperPort()
    {
        return zookeeper.getPort();
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
        return zookeeper.getConnectString();
    }

    public <K, V> RecordMetadata sendMessages(Stream<ProducerRecord<K, V>> recordStream)
    {
        return sendMessages(recordStream, ImmutableMap.of());
    }
    public <K, V> RecordMetadata sendMessages(Stream<ProducerRecord<K, V>> recordStream, Map<String, String> extraProducerProperties)
    {
        try (KafkaProducer<K, V> producer = createProducer(extraProducerProperties)) {
            Future<RecordMetadata> future = recordStream.map(record -> send(producer, record))
                    .reduce((first, second) -> second)
                    .orElse(Futures.immediateFuture(null));
            producer.flush();
            return future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private <K, V> Future<RecordMetadata> send(KafkaProducer<K, V> producer, ProducerRecord<K, V> record)
    {
        return Failsafe.with(
                new RetryPolicy<>()
                        .onRetry(event -> log.warn(event.getLastFailure(), "Retrying message send"))
                        .withMaxAttempts(10)
                        .withBackoff(1, 10_000, MILLIS))
                .get(() -> producer.send(record));
    }

    public void createTopicWithConfig(int partitions, int replication, String topic, boolean enableLogAppendTime)
    {
        checkState(started.get() && !stopped.get(), "not started!");

        Properties topicProperties = new Properties();
        if (enableLogAppendTime) {
            topicProperties.put("message.timestamp.type", "LogAppendTime");
        }
        ZkUtils zkUtils = ZkUtils.apply(getZookeeperConnectString(), 30_000, 30_000, false);
        try {
            AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicProperties, RackAwareMode.Disabled$.MODULE$);
        }
        finally {
            zkUtils.close();
        }
    }
}
